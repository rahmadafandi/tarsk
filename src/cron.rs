//! Five-field cron, evaluated in UTC.
//!
//! UTC is the whole of the timezone story here, and that is a decision rather
//! than an omission: a local-time schedule has to answer what "02:30 daily"
//! means on the night a clock jumps, and every answer is somebody's outage.
//! Schedules that must follow a wall clock belong in the layer that knows which
//! wall.
//!
//! Hand-written because the only question being asked is "does this minute
//! match", and the arithmetic behind it is a known algorithm with known answers
//! — see the tests at the bottom.

/// A parsed expression: one bitset per field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    minute: u64, // bits 0..=59
    hour: u32,   // bits 0..=23
    dom: u32,    // bits 1..=31
    month: u16,  // bits 1..=12
    dow: u8,     // bits 0..=6, Sunday is 0
    /// Cron ORs day-of-month with day-of-week when both are restricted, and
    /// ANDs them when only one is. Getting this backwards is the classic way a
    /// hand-rolled cron fires on the wrong days.
    dom_restricted: bool,
    dow_restricted: bool,
}

fn parse_field(spec: &str, min: u32, max: u32, name: &str) -> Result<(u64, bool), String> {
    let mut bits = 0u64;
    let mut restricted = true;
    for part in spec.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => (
                range,
                step.parse::<u32>()
                    .map_err(|_| format!("{name}: bad step in {part:?}"))?,
            ),
            None => (part, 1),
        };
        if step == 0 {
            return Err(format!("{name}: step of zero in {part:?}"));
        }
        let (lo, hi) = if range == "*" {
            restricted = false;
            (min, max)
        } else if let Some((lo, hi)) = range.split_once('-') {
            (
                lo.parse()
                    .map_err(|_| format!("{name}: bad range {part:?}"))?,
                hi.parse()
                    .map_err(|_| format!("{name}: bad range {part:?}"))?,
            )
        } else {
            let value: u32 = range
                .parse()
                .map_err(|_| format!("{name}: {range:?} is not a number"))?;
            (value, value)
        };
        if lo < min || hi > max || lo > hi {
            return Err(format!("{name}: {part:?} is outside {min}-{max}"));
        }
        let mut value = lo;
        while value <= hi {
            bits |= 1u64 << value;
            value += step;
        }
    }
    if bits == 0 {
        return Err(format!("{name}: {spec:?} matches nothing"));
    }
    Ok((bits, restricted))
}

pub fn parse(expr: &str) -> Result<Schedule, String> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(format!(
            "expected 5 fields (minute hour day-of-month month day-of-week), got {}",
            fields.len()
        ));
    }
    let (minute, _) = parse_field(fields[0], 0, 59, "minute")?;
    let (hour, _) = parse_field(fields[1], 0, 23, "hour")?;
    let (dom, dom_restricted) = parse_field(fields[2], 1, 31, "day-of-month")?;
    let (month, _) = parse_field(fields[3], 1, 12, "month")?;
    let (dow, dow_restricted) = parse_field(fields[4], 0, 6, "day-of-week")?;
    Ok(Schedule {
        minute,
        hour: hour as u32,
        dom: dom as u32,
        month: month as u16,
        dow: dow as u8,
        dom_restricted,
        dow_restricted,
    })
}

/// Civil date and time from a Unix timestamp, UTC. Howard Hinnant's
/// days-from-civil, run backwards.
fn civil(ts: i64) -> (u32, u32, u32, u32, u32) {
    let days = ts.div_euclid(86_400);
    let secs = ts.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let _year = y + i64::from(month <= 2);
    // 1970-01-01 was a Thursday, so shift by 4 to put Sunday at 0.
    let weekday = (days + 4).rem_euclid(7) as u32;
    (
        month,
        day,
        (secs / 3600) as u32,
        (secs % 3600 / 60) as u32,
        weekday,
    )
}

impl Schedule {
    /// Does the minute containing `ts` match?
    pub fn matches(&self, ts: i64) -> bool {
        let (month, day, hour, minute, weekday) = civil(ts);
        if self.minute & (1u64 << minute) == 0 {
            return false;
        }
        if self.hour & (1u32 << hour) == 0 {
            return false;
        }
        if self.month & (1u16 << month) == 0 {
            return false;
        }
        let by_dom = self.dom & (1u32 << day) != 0;
        let by_dow = self.dow & (1u8 << weekday) != 0;
        match (self.dom_restricted, self.dow_restricted) {
            (true, true) => by_dom || by_dow, // cron ORs them, oddly but famously
            (true, false) => by_dom,
            (false, true) => by_dow,
            (false, false) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-08-13T17:04:00Z, a Thursday.
    const THU: i64 = 1_786_640_640;

    #[test]
    fn civil_decodes_a_known_instant() {
        assert_eq!(civil(THU), (8, 13, 17, 4, 4)); // Aug 13, 17:04, Thursday
        assert_eq!(civil(0), (1, 1, 0, 0, 4)); // the epoch was a Thursday
                                               // 2024-02-29, a leap day that a naive calendar gets wrong
        assert_eq!(civil(1_709_208_000), (2, 29, 12, 0, 4));
    }

    #[test]
    fn every_minute() {
        let s = parse("* * * * *").unwrap();
        assert!(s.matches(THU));
        assert!(s.matches(THU + 60));
    }

    #[test]
    fn steps_and_lists() {
        let s = parse("*/5 * * * *").unwrap();
        assert!(s.matches(THU - 4 * 60)); // :00
        assert!(!s.matches(THU)); // :04
        let s = parse("0,30 * * * *").unwrap();
        assert!(s.matches(THU - 4 * 60));
        assert!(!s.matches(THU));
    }

    #[test]
    fn ranges_bind_the_hour() {
        let s = parse("4 9-17 * * *").unwrap();
        assert!(s.matches(THU)); // 17:04
        assert!(!s.matches(THU + 3 * 3600)); // 20:04
    }

    #[test]
    fn day_fields_are_ored_when_both_are_restricted() {
        // The 1st or any Thursday — cron's OR, not an AND.
        let s = parse("4 17 1 * 4").unwrap();
        assert!(s.matches(THU)); // a Thursday that is not the 1st
        let s = parse("4 17 13 * 1").unwrap();
        assert!(s.matches(THU)); // the 13th, on a day that is not Monday
        let s = parse("4 17 12 * 1").unwrap();
        assert!(!s.matches(THU)); // neither the 12th nor a Monday
    }

    #[test]
    fn day_of_month_alone_still_ands_with_the_rest() {
        let s = parse("4 17 13 8 *").unwrap();
        assert!(s.matches(THU));
        let s = parse("4 17 14 8 *").unwrap();
        assert!(!s.matches(THU));
    }

    #[test]
    fn bad_expressions_say_why() {
        for (expr, needle) in [
            ("* * * *", "5 fields"),
            ("60 * * * *", "outside 0-59"),
            ("* 25 * * *", "outside 0-23"),
            ("*/0 * * * *", "step of zero"),
            ("x * * * *", "not a number"),
        ] {
            let err = parse(expr).unwrap_err();
            assert!(err.contains(needle), "{expr:?} said {err:?}");
        }
    }
}
