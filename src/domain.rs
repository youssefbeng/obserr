use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, SecondsFormat, TimeZone, Timelike, Utc,
};
use serde::{Deserialize, Serialize};

pub type Source = String;
pub type Step = String;
pub type FailedRun = Run;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Abandoned,
}

impl RunStatus {
    /// Retourne la représentation textuelle canonique du statut d'un `Run`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }

    /// Reconstruit un `RunStatus` depuis la valeur stockée en base SQLite.
    pub fn from_db(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "abandoned" => Self::Abandoned,
            _ => Self::Failed,
        }
    }

    /// Indique si ce statut constitue un `Failed Run` (`failed` ou `abandoned` selon `GLOSSARY.md`).
    pub fn is_failed_run(&self) -> bool {
        matches!(self, Self::Failed | Self::Abandoned)
    }
}

/// Table de transitions IANA TZif v2/v3 chargée depuis `/usr/share/zoneinfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TzifData {
    transitions: Vec<i64>,
    type_indices: Vec<u8>,
    utoffs: Vec<i32>,
    default_utoff: i32,
}

impl TzifData {
    /// Parse un fichier binaire TZif v2/v3 (base IANA `/usr/share/zoneinfo/<name>`).
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 44 || &bytes[0..4] != b"TZif" {
            return None;
        }
        let read_u32 = |off: usize| -> Option<usize> {
            let b: [u8; 4] = bytes.get(off..off + 4)?.try_into().ok()?;
            Some(u32::from_be_bytes(b) as usize)
        };
        let read_i32 = |off: usize| -> Option<i32> {
            let b: [u8; 4] = bytes.get(off..off + 4)?.try_into().ok()?;
            Some(i32::from_be_bytes(b))
        };
        let read_i64 = |off: usize| -> Option<i64> {
            let b: [u8; 8] = bytes.get(off..off + 8)?.try_into().ok()?;
            Some(i64::from_be_bytes(b))
        };

        let isutcnt1 = read_u32(20)?;
        let isstdcnt1 = read_u32(24)?;
        let leapcnt1 = read_u32(28)?;
        let timecnt1 = read_u32(32)?;
        let typecnt1 = read_u32(36)?;
        let charcnt1 = read_u32(40)?;

        let v1_data_len = timecnt1 * 5 + typecnt1 * 6 + charcnt1 + leapcnt1 * 8 + isstdcnt1 + isutcnt1;
        let v2_hdr = 44 + v1_data_len;
        if bytes.get(v2_hdr..v2_hdr + 4) != Some(b"TZif") {
            return None;
        }

        let timecnt2 = read_u32(v2_hdr + 32)?;
        let typecnt2 = read_u32(v2_hdr + 36)?;
        if typecnt2 == 0 {
            return None;
        }

        let mut pos = v2_hdr + 44;
        let mut transitions = Vec::with_capacity(timecnt2);
        for _ in 0..timecnt2 {
            transitions.push(read_i64(pos)?);
            pos += 8;
        }

        let type_indices = bytes.get(pos..pos + timecnt2)?.to_vec();
        pos += timecnt2;

        let mut utoffs = Vec::with_capacity(typecnt2);
        let mut first_std_utoff = None;
        for _ in 0..typecnt2 {
            let utoff = read_i32(pos)?;
            let is_dst = *bytes.get(pos + 4)?;
            if is_dst == 0 && first_std_utoff.is_none() {
                first_std_utoff = Some(utoff);
            }
            utoffs.push(utoff);
            pos += 6;
        }

        let default_utoff = first_std_utoff.unwrap_or(utoffs[0]);
        Some(Self {
            transitions,
            type_indices,
            utoffs,
            default_utoff,
        })
    }

    fn offset_seconds(&self, utc_ts: i64) -> i32 {
        let idx = self.transitions.partition_point(|&t| t <= utc_ts);
        if idx == 0 {
            self.default_utoff
        } else {
            let type_idx = self.type_indices[idx - 1] as usize;
            self.utoffs.get(type_idx).copied().unwrap_or(self.default_utoff)
        }
    }
}

/// Fuseau horaire IANA supportant toute la base `/usr/share/zoneinfo` ainsi que les règles DST intégrées.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Timezone {
    Utc,
    Tzif(Arc<TzifData>),
    EuCentral,
    EuWestern,
    EuEastern,
    UsZone { std_offset_secs: i32 },
    Fixed { offset_secs: i32 },
}

/// Parse un nom de fuseau horaire IANA (ex. `"UTC"`, `"Europe/Paris"`, `"Australia/Sydney"`) ou un décalage fixe (`"+02:00"`).
pub fn parse_timezone(name: &str) -> Result<Timezone> {
    let trimmed = name.trim();
    if matches!(trimmed, "UTC" | "Etc/UTC" | "GMT" | "Etc/GMT" | "Z") {
        return Ok(Timezone::Utc);
    }

    if !trimmed.starts_with('/') && !trimmed.contains("..") {
        let zpath = Path::new("/usr/share/zoneinfo").join(trimmed);
        if let Ok(bytes) = std::fs::read(zpath) {
            if let Some(tzif) = TzifData::from_bytes(&bytes) {
                return Ok(Timezone::Tzif(Arc::new(tzif)));
            }
        }
    }

    match trimmed {
        "Europe/Paris" | "Europe/Berlin" | "Europe/Brussels" | "Europe/Amsterdam"
        | "Europe/Madrid" | "Europe/Rome" | "Europe/Zurich" | "CET" | "MET" => {
            Ok(Timezone::EuCentral)
        }
        "Europe/London" | "Europe/Dublin" | "Europe/Lisbon" | "WET" => Ok(Timezone::EuWestern),
        "Europe/Athens" | "Europe/Helsinki" | "Europe/Bucharest" | "EET" => Ok(Timezone::EuEastern),
        "America/New_York" | "US/Eastern" => Ok(Timezone::UsZone {
            std_offset_secs: -5 * 3600,
        }),
        "America/Chicago" | "US/Central" => Ok(Timezone::UsZone {
            std_offset_secs: -6 * 3600,
        }),
        "America/Denver" | "US/Mountain" => Ok(Timezone::UsZone {
            std_offset_secs: -7 * 3600,
        }),
        "America/Los_Angeles" | "US/Pacific" => Ok(Timezone::UsZone {
            std_offset_secs: -8 * 3600,
        }),
        _ => {
            if let Some(offset_secs) = parse_fixed_offset_str(trimmed) {
                Ok(Timezone::Fixed { offset_secs })
            } else {
                Err(anyhow!("unknown timezone '{trimmed}'"))
            }
        }
    }
}

fn parse_fixed_offset_str(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    if bytes.len() != 6 || (bytes[0] != b'+' && bytes[0] != b'-') || bytes[3] != b':' {
        return None;
    }
    let sign = if bytes[0] == b'-' { -1 } else { 1 };
    let hours: i32 = s[1..3].parse().ok()?;
    let mins: i32 = s[4..6].parse().ok()?;
    if hours > 23 || mins > 59 {
        return None;
    }
    Some(sign * (hours * 3600 + mins * 60))
}

impl Timezone {
    /// Calcule le décalage UTC en secondes à l'instant `utc`, en tenant compte des changements d'heure d'été/hiver (`DST`).
    pub fn offset_seconds(&self, utc: DateTime<Utc>) -> i32 {
        match self {
            Self::Utc => 0,
            Self::Tzif(data) => data.offset_seconds(utc.timestamp()),
            Self::EuCentral => {
                if is_eu_dst(utc) {
                    2 * 3600
                } else {
                    3600
                }
            }
            Self::EuWestern => {
                if is_eu_dst(utc) {
                    3600
                } else {
                    0
                }
            }
            Self::EuEastern => {
                if is_eu_dst(utc) {
                    3 * 3600
                } else {
                    2 * 3600
                }
            }
            Self::UsZone { std_offset_secs } => {
                if is_us_dst(utc, *std_offset_secs) {
                    *std_offset_secs + 3600
                } else {
                    *std_offset_secs
                }
            }
            Self::Fixed { offset_secs } => *offset_secs,
        }
    }

    /// Convertit un horodatage `UTC` dans le fuseau horaire local correspondant.
    pub fn to_local(&self, utc: DateTime<Utc>) -> DateTime<FixedOffset> {
        let offset = FixedOffset::east_opt(self.offset_seconds(utc))
            .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
        utc.with_timezone(&offset)
    }
}

/// Règle européenne d'heure d'été : du dernier dimanche de mars à 01:00 UTC au dernier dimanche d'octobre à 01:00 UTC.
fn is_eu_dst(utc: DateTime<Utc>) -> bool {
    let year = utc.year();
    let march_last_sun = last_sunday_of_month(year, 3);
    let oct_last_sun = last_sunday_of_month(year, 10);

    let dst_start = Utc.from_utc_datetime(&march_last_sun.and_hms_opt(1, 0, 0).unwrap());
    let dst_end = Utc.from_utc_datetime(&oct_last_sun.and_hms_opt(1, 0, 0).unwrap());

    utc >= dst_start && utc < dst_end
}

/// Règle nord-américaine d'heure d'été : du 2e dimanche de mars (02:00 heure standard) au 1er dimanche de novembre (02:00 heure d'été).
fn is_us_dst(utc: DateTime<Utc>, std_offset_secs: i32) -> bool {
    let year = utc.year();
    let march_second_sun = nth_sunday_of_month(year, 3, 2);
    let nov_first_sun = nth_sunday_of_month(year, 11, 1);

    let dst_start_local = march_second_sun.and_hms_opt(2, 0, 0).unwrap();
    let dst_start =
        Utc.from_utc_datetime(&(dst_start_local - Duration::seconds(std_offset_secs as i64)));

    let dst_end_local = nov_first_sun.and_hms_opt(2, 0, 0).unwrap();
    let dst_end = Utc.from_utc_datetime(
        &(dst_end_local - Duration::seconds((std_offset_secs + 3600) as i64)),
    );

    utc >= dst_start && utc < dst_end
}

fn last_sunday_of_month(year: i32, month: u32) -> NaiveDate {
    let last_day = match month {
        3 | 10 => 31,
        _ => 30,
    };
    let date = NaiveDate::from_ymd_opt(year, month, last_day).unwrap();
    let days_since_sun = date.weekday().num_days_from_sunday();
    date - Duration::days(days_since_sun as i64)
}

fn nth_sunday_of_month(year: i32, month: u32, nth: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    let days_until_sun = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + Duration::days((days_until_sun + (nth - 1) * 7) as i64)
}

/// Représente le `Schedule` d'un `Job` (expression cron à 5 champs, délai de tolérance `grace` et fuseau horaire).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    pub cron: String,
    pub grace_seconds: i64,
    pub timezone_name: String,
    timezone: Timezone,
    parsed: ParsedCron,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedCron {
    minutes: u64,
    hours: u32,
    days_of_month: u32,
    months: u16,
    days_of_week: u8,
    dom_wildcard: bool,
    dow_wildcard: bool,
}

impl Schedule {
    /// Construit et valide un `Schedule` complet à partir de son expression cron, de son délai `grace` et de son fuseau horaire.
    pub fn new(
        cron: impl Into<String>,
        grace_seconds: i64,
        timezone_name: impl Into<String>,
    ) -> Result<Self> {
        let cron_str = cron.into();
        let tz_name = timezone_name.into();
        let parsed = Self::parse_cron_internal(&cron_str)?;
        let timezone = parse_timezone(&tz_name)?;
        Ok(Self {
            cron: cron_str,
            grace_seconds: grace_seconds.max(0),
            timezone_name: tz_name,
            timezone,
            parsed,
        })
    }

    /// Valide la syntaxe d'une expression cron standard à 5 champs (`min hour dom month dow`).
    pub fn parse_cron(expr: &str) -> Result<()> {
        Self::parse_cron_internal(expr).map(|_| ())
    }

    fn parse_cron_internal(expr: &str) -> Result<ParsedCron> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        let [min_str, hour_str, dom_str, mon_str, dow_str] = parts.as_slice() else {
            return Err(anyhow!(
                "cron expression must have 5 fields (min hour dom month dow), got '{}'",
                expr
            ));
        };

        let minutes = parse_cron_field(min_str, 0, 59, false)?;
        let hours = parse_cron_field(hour_str, 0, 23, false)? as u32;
        let days_of_month = parse_cron_field(dom_str, 1, 31, false)? as u32;
        let months = parse_cron_field(mon_str, 1, 12, false)? as u16;
        let days_of_week = parse_cron_field(dow_str, 0, 7, true)? as u8;

        Ok(ParsedCron {
            minutes,
            hours,
            days_of_month,
            months,
            days_of_week,
            dom_wildcard: *dom_str == "*",
            dow_wildcard: *dow_str == "*",
        })
    }

    /// Vérifie si un instant `UTC` correspond à un créneau théorique de ce `Schedule` dans son fuseau horaire.
    pub fn matches(&self, utc: DateTime<Utc>) -> bool {
        if utc.second() != 0 {
            return false;
        }

        let local = self.timezone.to_local(utc);
        if (self.parsed.minutes & (1u64 << local.minute())) == 0 {
            return false;
        }
        if (self.parsed.hours & (1u32 << local.hour())) == 0 {
            return false;
        }
        if (self.parsed.months & (1u16 << local.month())) == 0 {
            return false;
        }

        let dom_match = (self.parsed.days_of_month & (1u32 << local.day())) != 0;
        let dow_num = local.weekday().num_days_from_sunday();
        let dow_match = (self.parsed.days_of_week & (1u8 << dow_num)) != 0;

        let day_ok = match (self.parsed.dom_wildcard, self.parsed.dow_wildcard) {
            (true, true) => true,
            (false, true) => dom_match,
            (true, false) => dow_match,
            (false, false) => dom_match || dow_match,
        };
        if !day_ok {
            return false;
        }

        // Lors du passage à l'heure d'hiver (répétition d'une heure locale), évite de déclencher deux fois la même heure locale.
        let one_hour_earlier = utc - Duration::hours(1);
        if self.timezone.to_local(one_hour_earlier).naive_local() == local.naive_local() {
            return false;
        }

        true
    }

    /// Calcule le prochain créneau attendu strictement après l'instant `after`.
    pub fn next_slot_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut ts = (after.timestamp().div_euclid(60) + 1) * 60;
        let limit = ts + 86_400 * 366;
        while ts <= limit {
            if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
                if self.matches(dt) {
                    return Some(dt);
                }
            }
            ts += 60;
        }
        None
    }

    /// Retourne tous les créneaux théoriques `T >= registered_at` (et `T > last_evaluated_slot`) dont le délai `T + grace <= now` est expiré.
    pub fn due_slots(
        &self,
        registered_at: DateTime<Utc>,
        last_evaluated_slot: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Vec<DateTime<Utc>> {
        let latest_due_slot = now - Duration::seconds(self.grace_seconds);
        let min_slot = match last_evaluated_slot {
            Some(last) => (last + Duration::seconds(60)).max(registered_at),
            None => registered_at,
        };

        if latest_due_slot < min_slot {
            return Vec::new();
        }

        let rem = min_slot.timestamp().rem_euclid(60);
        let start_ts = if rem == 0 {
            min_slot.timestamp()
        } else {
            min_slot.timestamp() + (60 - rem)
        };
        let end_ts = latest_due_slot.timestamp();

        let mut slots = Vec::new();
        let mut ts = start_ts;
        while ts <= end_ts {
            if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
                if self.matches(dt) {
                    slots.push(dt);
                }
            }
            ts += 60;
        }
        slots
    }
}

fn parse_cron_field(field: &str, min: u32, max: u32, is_dow: bool) -> Result<u64> {
    let mut mask = 0u64;
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(anyhow!("empty cron field segment in '{field}'"));
        }
        let (range_part, step) = match part.split_once('/') {
            Some((r, s)) => {
                let st: u32 = s
                    .parse()
                    .map_err(|_| anyhow!("invalid cron step '{s}' in '{field}'"))?;
                if st == 0 {
                    return Err(anyhow!("cron step cannot be 0 in '{field}'"));
                }
                (r, st)
            }
            None => (part, 1),
        };

        let (start, end) = if range_part == "*" {
            (min, max)
        } else if let Some((a, b)) = range_part.split_once('-') {
            let s_val: u32 = a
                .parse()
                .map_err(|_| anyhow!("invalid cron range start '{a}' in '{field}'"))?;
            let e_val: u32 = b
                .parse()
                .map_err(|_| anyhow!("invalid cron range end '{b}' in '{field}'"))?;
            if s_val < min || e_val > max || s_val > e_val {
                return Err(anyhow!("cron range '{range_part}' out of bounds [{min}..={max}]"));
            }
            (s_val, e_val)
        } else {
            let val: u32 = range_part
                .parse()
                .map_err(|_| anyhow!("invalid cron value '{range_part}' in '{field}'"))?;
            if val < min || val > max {
                return Err(anyhow!("cron value '{val}' out of bounds [{min}..={max}]"));
            }
            if part.contains('/') {
                (val, max)
            } else {
                (val, val)
            }
        };

        let mut v = start;
        while v <= end {
            let bit = if is_dow && v == 7 { 0 } else { v };
            mask |= 1u64 << bit;
            v += step;
        }
    }
    Ok(mask)
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: i64,
    pub source: Source,
    pub name: String,
    pub schedule_cron: Option<String>,
    pub grace_seconds: Option<i64>,
    pub timezone: Option<String>,
    pub owner: Option<String>,
    pub tags: Vec<String>,
    pub schedule_registered_at: Option<DateTime<Utc>>,
    pub last_scheduled_slot: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl Job {
    /// Retourne le fuseau horaire effectif du `Job` (fuseau spécifique au `Job` ou fuseau global par défaut).
    pub fn effective_timezone<'a>(&'a self, default_timezone: &'a str) -> &'a str {
        self.timezone
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(default_timezone)
    }

    /// Construit l'objet métier `Schedule` du `Job` s'il possède une expression cron configurée.
    pub fn schedule(&self, default_timezone: &str) -> Option<Schedule> {
        let cron = self.schedule_cron.as_deref()?;
        let grace = self.grace_seconds.unwrap_or(0);
        let tz = self.effective_timezone(default_timezone);
        Schedule::new(cron, grace, tz).ok()
    }

    /// Retourne le statut métier du `Job` (`Incident ouvert`, `OK` ou `Sans Schedule`).
    pub fn status_label(&self, has_open_incident: bool) -> &'static str {
        if has_open_incident {
            "Incident ouvert"
        } else if self.schedule_cron.is_some() {
            "OK"
        } else {
            "Sans Schedule"
        }
    }
}

/// Un `Run` attendu par le `Schedule` qui n'a pas démarré (ou terminé) avant l'expiration du délai de tolérance (`GLOSSARY.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissedRun {
    pub id: i64,
    pub job_id: i64,
    pub scheduled_for: DateTime<Utc>,
    pub detected_at: DateTime<Utc>,
}

impl MissedRun {
    /// Formate la description métier d'un créneau manqué pour un `Missed Run`.
    pub fn format_slot_excerpt(scheduled_for: DateTime<Utc>) -> String {
        let scheduled_str = scheduled_for.to_rfc3339_opts(SecondsFormat::Secs, true);
        format!("Missed Run pour le créneau attendu {scheduled_str}")
    }

    /// Retourne le résumé textuel de ce `Missed Run` affiché sur un `Incident` ouvert.
    pub fn summary_text(&self) -> String {
        Self::format_slot_excerpt(self.scheduled_for)
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub id: i64,
    pub job_id: i64,
    pub external_run_id: Option<String>,
    pub status: RunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failed_step: Option<Step>,
    pub error_excerpt: Option<String>,
    pub external_url: Option<String>,
}

impl Run {
    /// Calcule la durée d'exécution en secondes lorsque le début et la fin du `Run` sont connus.
    pub fn duration_seconds(&self) -> Option<i64> {
        let start = self.started_at?;
        let end = self.finished_at?;
        Some((end - start).num_seconds().max(0))
    }

    /// Indique si ce `Run` est comptabilisé comme un `Failed Run` (`failed` ou `abandoned`).
    pub fn is_failed_run(&self) -> bool {
        self.status.is_failed_run()
    }

    /// Horodatage effectif utilisé pour vérifier la couverture d'un créneau de `Schedule` (`started_at`, ou `finished_at` si absent).
    pub fn coverage_timestamp(&self) -> Option<DateTime<Utc>> {
        self.started_at.or(self.finished_at)
    }
}

/// Indique si au moins un `Run` de la liste couvre la fenêtre `[slot, slot + grace]`.
pub fn is_slot_covered_by_runs(runs: &[Run], slot: DateTime<Utc>, grace_seconds: i64) -> bool {
    let deadline = slot + Duration::seconds(grace_seconds.max(0));
    runs.iter().any(|run| {
        run.coverage_timestamp()
            .is_some_and(|ts| ts >= slot && ts <= deadline)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus {
    Open,
    Closed,
}

impl IncidentStatus {
    /// Retourne la représentation textuelle canonique du statut d'un `Incident`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }

    /// Reconstruit un `IncidentStatus` depuis la valeur stockée en base SQLite.
    pub fn from_db(value: &str) -> Self {
        match value {
            "closed" => Self::Closed,
            _ => Self::Open,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncidentCause {
    FailedRun,
    AbandonedRun,
    MissedRun,
}

impl IncidentCause {
    /// Retourne le code stocké en base SQLite pour la cause initiale d'un `Incident`.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::FailedRun => "failed_run",
            Self::AbandonedRun => "abandoned_run",
            Self::MissedRun => "missed_run",
        }
    }

    /// Reconstruit un `IncidentCause` depuis la valeur stockée en base SQLite.
    pub fn from_db(value: &str) -> Self {
        match value {
            "abandoned_run" => Self::AbandonedRun,
            "missed_run" => Self::MissedRun,
            _ => Self::FailedRun,
        }
    }

    /// Retourne le libellé métier affiché dans l'UI et les alertes de `Channel`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::FailedRun => "Failed Run (failed)",
            Self::AbandonedRun => "Failed Run (abandoned — Run abandonné)",
            Self::MissedRun => "Missed Run",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Incident {
    pub id: i64,
    pub job_id: i64,
    pub status: IncidentStatus,
    pub cause: IncidentCause,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub opening_run_id: Option<i64>,
    pub closing_run_id: Option<i64>,
}

impl Incident {
    /// Calcule la durée totale de l'`Incident` en secondes lorsqu'il est fermé.
    pub fn duration_seconds(&self) -> Option<i64> {
        let closed = self.closed_at?;
        Some((closed - self.opened_at).num_seconds().max(0))
    }
}

/// Notification métier émise par le moteur d'`Incidents` à destination d'un `Channel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentNotification {
    Opened {
        source: Source,
        job_name: String,
        cause: IncidentCause,
        opened_at: DateTime<Utc>,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
        external_url: Option<String>,
    },
    Resolved {
        source: Source,
        job_name: String,
        opened_at: DateTime<Utc>,
        closed_at: DateTime<Utc>,
        duration_seconds: i64,
    },
}

/// Transition d'état d'un `Incident` décidée par le cœur métier (`ADR-0005`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentTransition {
    None,
    Open {
        cause: IncidentCause,
        opening_run_id: Option<i64>,
        notification: IncidentNotification,
    },
    Close {
        incident_id: i64,
        closing_run_id: i64,
        notification: IncidentNotification,
    },
}

/// Contexte d'un `Run` terminé transmis au moteur d'évaluation des `Incidents`.
#[derive(Debug, Clone)]
pub struct CompletedRunOutcome {
    pub run_id: i64,
    pub status: RunStatus,
    pub failed_step: Option<Step>,
    pub error_excerpt: Option<String>,
    pub external_url: Option<String>,
}

/// Évalue la transition d'`Incident` provoquée par un `Run` terminé (`Failed Run` ou `succeeded`).
pub fn evaluate_incident_transition(
    open_incident: Option<&Incident>,
    event: &IngestEvent,
    completed: CompletedRunOutcome,
) -> IncidentTransition {
    if completed.status.is_failed_run() {
        if open_incident.is_none() {
            let cause = match completed.status {
                RunStatus::Abandoned => IncidentCause::AbandonedRun,
                _ => IncidentCause::FailedRun,
            };
            return IncidentTransition::Open {
                cause,
                opening_run_id: Some(completed.run_id),
                notification: IncidentNotification::Opened {
                    source: event.source.clone(),
                    job_name: event.job_name.clone(),
                    cause,
                    opened_at: event.timestamp,
                    failed_step: completed.failed_step,
                    error_excerpt: completed.error_excerpt,
                    external_url: completed.external_url,
                },
            };
        }
        return IncidentTransition::None;
    }

    if completed.status == RunStatus::Succeeded {
        if let Some(inc) = open_incident {
            let duration_seconds = (event.timestamp - inc.opened_at).num_seconds().max(0);
            return IncidentTransition::Close {
                incident_id: inc.id,
                closing_run_id: completed.run_id,
                notification: IncidentNotification::Resolved {
                    source: event.source.clone(),
                    job_name: event.job_name.clone(),
                    opened_at: inc.opened_at,
                    closed_at: event.timestamp,
                    duration_seconds,
                },
            };
        }
    }

    IncidentTransition::None
}

/// Évalue la transition d'`Incident` provoquée par la détection d'un `Missed Run` à l'instant `now`.
pub fn evaluate_missed_run_incident(
    open_incident: Option<&Incident>,
    job: &Job,
    scheduled_for: DateTime<Utc>,
    now: DateTime<Utc>,
) -> IncidentTransition {
    if open_incident.is_some() {
        return IncidentTransition::None;
    }
    IncidentTransition::Open {
        cause: IncidentCause::MissedRun,
        opening_run_id: None,
        notification: IncidentNotification::Opened {
            source: job.source.clone(),
            job_name: job.name.clone(),
            cause: IncidentCause::MissedRun,
            opened_at: now,
            failed_step: None,
            error_excerpt: Some(MissedRun::format_slot_excerpt(scheduled_for)),
            external_url: None,
        },
    }
}

#[derive(Debug, Clone)]
pub struct JobWithLatestRun {
    pub job: Job,
    pub latest_run: Option<Run>,
    pub open_incident: Option<Incident>,
    pub effective_timezone: String,
    pub next_expected_slot: Option<DateTime<Utc>>,
}

impl JobWithLatestRun {
    /// Retourne le statut métier du `Job` (`Incident ouvert`, `OK` ou `Sans Schedule`).
    pub fn status_label(&self) -> &'static str {
        self.job.status_label(self.open_incident.is_some())
    }

    /// Rang de tri sur le Tableau de bord : `Incident ouvert` (0), puis `OK` (1), puis `Sans Schedule` (2).
    pub fn sort_rank(&self) -> u8 {
        if self.open_incident.is_some() {
            0
        } else if self.job.schedule_cron.is_some() {
            1
        } else {
            2
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenIncidentSummary {
    pub incident: Incident,
    pub job: Job,
    pub latest_error_excerpt: Option<String>,
    pub external_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JobDetails {
    pub job: Job,
    pub open_incident: Option<Incident>,
    pub effective_timezone: String,
    pub next_expected_slot: Option<DateTime<Utc>>,
    pub runs: Vec<Run>,
    pub missed_runs: Vec<MissedRun>,
    pub incidents: Vec<Incident>,
}

impl JobDetails {
    /// Retourne le statut métier courant du `Job` (`Incident ouvert`, `OK` ou `Sans Schedule`).
    pub fn status_label(&self) -> &'static str {
        self.job.status_label(self.open_incident.is_some())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FinishedOutcome {
    Succeeded,
    Failed,
}

impl FinishedOutcome {
    /// Convertit le résultat d'un événement `finished` en statut de `Run`.
    pub fn into_run_status(self) -> RunStatus {
        match self {
            Self::Succeeded => RunStatus::Succeeded,
            Self::Failed => RunStatus::Failed,
        }
    }
}

#[derive(Debug, Clone)]
pub enum IngestPayloadKind {
    Started,
    Finished {
        outcome: FinishedOutcome,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct IngestEvent {
    pub source: Source,
    pub job_name: String,
    pub run_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub external_url: Option<String>,
    pub kind: IngestPayloadKind,
}

/// Décision métier pure produite par le domaine lors de la corrélation d'un événement (`ADR-0005`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunCorrelation {
    /// Abandonne les `Runs` en cours listés (identifiants internes) et insère un nouveau `Run` `running`.
    StartNewRun { abandon_run_ids: Vec<i64> },
    /// Événement `started` idempotent sur un `Run` déjà `running` avec le même `run_id`.
    KeepRunningRun { existing_run_id: i64 },
    /// Met à jour un `Run` en cours existant avec son état de fin.
    FinishExistingRun {
        run_id: i64,
        status: RunStatus,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
    },
    /// Crée directement un `Run` terminé sans horodatage de début (`finished` reçu seul).
    InsertFinishedRunWithoutStart {
        status: RunStatus,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
    },
}

/// Détermine l'action de corrélation (`RunCorrelation`) à appliquer face aux `Runs` en cours d'un `Job`.
pub fn correlate_event(running_runs: &[Run], event: &IngestEvent) -> RunCorrelation {
    match &event.kind {
        IngestPayloadKind::Started => {
            let mut abandon_run_ids = Vec::new();
            let mut same_run_id = None;

            for prev in running_runs {
                let is_same = match (prev.external_run_id.as_deref(), event.run_id.as_deref()) {
                    (Some(existing), Some(incoming)) => existing == incoming,
                    _ => false,
                };
                if is_same {
                    same_run_id = Some(prev.id);
                } else {
                    abandon_run_ids.push(prev.id);
                }
            }

            if let Some(existing_run_id) = same_run_id {
                RunCorrelation::KeepRunningRun { existing_run_id }
            } else {
                RunCorrelation::StartNewRun { abandon_run_ids }
            }
        }
        IngestPayloadKind::Finished {
            outcome,
            failed_step,
            error_excerpt,
        } => {
            let status = outcome.into_run_status();
            let truncated_excerpt = error_excerpt.as_deref().map(truncate_error_excerpt);

            let matched_run = match event.run_id.as_deref() {
                Some(ext_id) => running_runs
                    .iter()
                    .rev()
                    .find(|r| r.external_run_id.as_deref() == Some(ext_id)),
                None => running_runs.last(),
            };

            if let Some(existing) = matched_run {
                RunCorrelation::FinishExistingRun {
                    run_id: existing.id,
                    status,
                    failed_step: failed_step.clone(),
                    error_excerpt: truncated_excerpt,
                }
            } else {
                RunCorrelation::InsertFinishedRunWithoutStart {
                    status,
                    failed_step: failed_step.clone(),
                    error_excerpt: truncated_excerpt,
                }
            }
        }
    }
}

/// Conserve uniquement les 20 dernières lignes d'un extrait d'erreur (`ADR-0003`).
pub fn truncate_error_excerpt(excerpt: &str) -> String {
    let lines: Vec<&str> = excerpt.lines().collect();
    let start = lines.len().saturating_sub(20);
    lines[start..].join("\n")
}
