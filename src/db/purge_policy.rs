use std::{collections::BTreeMap, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgeSetting {
    Retain(Duration),
    Never,
    Purge,
}

#[derive(Debug, Clone, Default)]
pub struct PurgePolicy {
    pub default_setting: Option<PurgeSetting>,
    pub per_kind: BTreeMap<u16, PurgeSetting>,
}

impl PurgePolicy {
    /// Return the retain duration configured for a specific kind, if any.
    pub fn purge_after_for_kind(&self, kind: u16) -> Option<Duration> {
        match self.setting_for_kind(kind) {
            Some(PurgeSetting::Retain(duration)) => Some(duration),
            _ => None,
        }
    }

    /// Whether events of a kind should be purged immediately on insert.
    pub fn should_purge_immediately(&self, kind: u16) -> bool {
        matches!(self.setting_for_kind(kind), Some(PurgeSetting::Purge))
    }

    /// Check if a kind overrides the default purge behavior.
    pub fn has_override(&self, kind: u16) -> bool {
        self.per_kind.contains_key(&kind)
    }

    /// Iterate over kinds that retain events for a custom duration.
    pub fn purge_overrides(&self) -> impl Iterator<Item = (u16, Duration)> + '_ {
        self.per_kind
            .iter()
            .filter_map(|(kind, setting)| match setting {
                PurgeSetting::Retain(duration) => Some((*kind, *duration)),
                PurgeSetting::Never | PurgeSetting::Purge => None,
            })
    }

    /// Return the default retention duration if one is configured.
    pub fn default_duration(&self) -> Option<Duration> {
        match self.default_setting {
            Some(PurgeSetting::Retain(duration)) => Some(duration),
            _ => None,
        }
    }

    fn setting_for_kind(&self, kind: u16) -> Option<PurgeSetting> {
        self.per_kind.get(&kind).copied().or(self.default_setting)
    }

    /// Parse purge policy definitions from CLI-style specifications.
    pub fn from_specs<I, S>(specs: I) -> Result<Self, PurgeSpecError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut options = Self::default();
        let mut seen_any = false;

        for spec in specs {
            let raw = spec.as_ref();
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(PurgeSpecError::invalid(raw, "empty specification"));
            }
            seen_any = true;

            if let Some((kind_part, window_part)) =
                trimmed.split_once(':').or_else(|| trimmed.split_once('='))
            {
                let kind_str = kind_part.trim();
                let window_str = window_part.trim();

                if kind_str.is_empty() || window_str.is_empty() {
                    return Err(PurgeSpecError::invalid(raw, "missing kind or window"));
                }

                let kind = kind_str
                    .parse::<u16>()
                    .map_err(|_| PurgeSpecError::invalid(raw, "invalid kind identifier"))?;

                if window_str.eq_ignore_ascii_case("never") {
                    options.per_kind.insert(kind, PurgeSetting::Never);
                    continue;
                }

                if window_str.eq_ignore_ascii_case("purge") {
                    options.per_kind.insert(kind, PurgeSetting::Purge);
                    continue;
                }

                let duration = parse_duration(window_str)
                    .map_err(|reason| PurgeSpecError::invalid(raw, reason))?;
                options
                    .per_kind
                    .insert(kind, PurgeSetting::Retain(duration));
            } else if trimmed.eq_ignore_ascii_case("never") {
                options.default_setting = Some(PurgeSetting::Never);
            } else if trimmed.eq_ignore_ascii_case("purge") {
                options.default_setting = Some(PurgeSetting::Purge);
            } else {
                let duration = parse_duration(trimmed)
                    .map_err(|reason| PurgeSpecError::invalid(raw, reason))?;
                options.default_setting = Some(PurgeSetting::Retain(duration));
            }
        }

        if !seen_any {
            return Err(PurgeSpecError::NoSpecs);
        }

        Ok(options)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PurgeSpecError {
    #[error("provide at least one purge specification")]
    NoSpecs,
    #[error("invalid purge specification '{spec}': {reason}")]
    InvalidSpec { spec: String, reason: String },
}

impl PurgeSpecError {
    fn invalid(spec: &str, reason: impl Into<String>) -> Self {
        Self::InvalidSpec {
            spec: spec.to_string(),
            reason: reason.into(),
        }
    }
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 3_600;
const SECONDS_PER_DAY: u64 = 86_400;
const SECONDS_PER_YEAR: u64 = SECONDS_PER_DAY * 365;

fn parse_duration(value: &str) -> Result<Duration, &'static str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("empty duration");
    }

    let last = trimmed.chars().last().ok_or("empty duration")?;

    if !last.is_ascii_alphabetic() {
        return Err("missing duration suffix");
    }

    let number_part = &trimmed[..trimmed.len() - 1];
    if number_part.is_empty() {
        return Err("missing duration value");
    }

    let multiplier = match last.to_ascii_lowercase() {
        'y' => SECONDS_PER_YEAR,
        'd' => SECONDS_PER_DAY,
        'h' => SECONDS_PER_HOUR,
        'm' => SECONDS_PER_MINUTE,
        's' => 1,
        _ => return Err("unknown duration suffix"),
    };

    let amount = number_part
        .parse::<u64>()
        .map_err(|_| "invalid duration value")?;
    let seconds = amount
        .checked_mul(multiplier)
        .ok_or("duration exceeds limit")?;
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::{PurgePolicy, PurgeSetting, PurgeSpecError, parse_duration};
    use std::time::Duration;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("1s").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("3h").unwrap(), Duration::from_secs(3 * 3600));
        assert_eq!(
            parse_duration("4d").unwrap(),
            Duration::from_secs(4 * 86_400)
        );
        assert_eq!(
            parse_duration("1y").unwrap(),
            Duration::from_secs(365 * 86_400)
        );
    }

    #[test]
    fn parse_duration_rejects_missing_suffix() {
        assert!(parse_duration("30").is_err());
        assert!(parse_duration("  ").is_err());
    }

    #[test]
    fn from_specs_applies_default_and_kind_overrides() {
        let policy = PurgePolicy::from_specs(["30d", "1:7d"]).expect("policy");
        assert_eq!(
            policy.default_duration(),
            Some(Duration::from_secs(30 * 86_400))
        );
        assert_eq!(
            policy.per_kind.get(&1),
            Some(&PurgeSetting::Retain(Duration::from_secs(7 * 86_400)))
        );
    }

    #[test]
    fn from_specs_handles_never() {
        let policy = PurgePolicy::from_specs(["never", "0:never", "1:3d"]).expect("policy");
        assert_eq!(policy.default_setting, Some(PurgeSetting::Never));
        assert_eq!(policy.per_kind.get(&0), Some(&PurgeSetting::Never));
        assert_eq!(
            policy.per_kind.get(&1),
            Some(&PurgeSetting::Retain(Duration::from_secs(3 * 86_400)))
        );
    }

    #[test]
    fn from_specs_handles_purge() {
        let policy = PurgePolicy::from_specs(["purge", "2:purge", "1:3d"]).expect("policy");
        assert_eq!(policy.default_setting, Some(PurgeSetting::Purge));
        assert!(policy.should_purge_immediately(2));
        assert!(policy.should_purge_immediately(3));
        assert_eq!(
            policy.purge_after_for_kind(1),
            Some(Duration::from_secs(3 * 86_400))
        );
    }

    #[test]
    fn from_specs_errors_on_empty_input() {
        let err = PurgePolicy::from_specs::<[&str; 0], _>([]).unwrap_err();
        matches!(err, PurgeSpecError::NoSpecs);
    }

    #[test]
    fn from_specs_errors_on_invalid_kind() {
        let err = PurgePolicy::from_specs(["abc:1d"]).unwrap_err();
        assert!(matches!(err, PurgeSpecError::InvalidSpec { .. }));
    }
}
