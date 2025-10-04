use std::collections::BTreeMap;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::{EventId, Kind, PublicKey, Timestamp};

#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub ids: Option<Vec<EventId>>,
    pub authors: Option<Vec<PublicKey>>,
    pub kinds: Option<Vec<Kind>>,
    pub since: Option<Timestamp>,
    pub until: Option<Timestamp>,
    pub limit: Option<usize>,
    pub search: Option<String>,
    pub generic_tags: Vec<(char, Vec<String>)>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RawFilter {
    #[serde(default)]
    ids: Option<Vec<EventId>>,
    #[serde(default)]
    authors: Option<Vec<PublicKey>>,
    #[serde(default)]
    kinds: Option<Vec<Kind>>,
    #[serde(default)]
    since: Option<Timestamp>,
    #[serde(default)]
    until: Option<Timestamp>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    search: Option<String>,
    #[serde(default)]
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl Filter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_value(value: Value) -> Result<Vec<Self>, serde_json::Error> {
        match value {
            Value::Array(values) => values
                .into_iter()
                .map(|value| match value {
                    Value::Object(map) => Self::from_raw_map(map),
                    other => Err(DeError::custom(format!(
                        "each filter must be an object, got {other:?}"
                    ))),
                })
                .collect(),
            Value::Object(map) => Self::from_raw_map(map).map(|filter| vec![filter]),
            other => Err(DeError::custom(format!(
                "filters must be array or object, got {other:?}"
            ))),
        }
    }

    fn from_raw_map(map: serde_json::Map<String, Value>) -> Result<Self, serde_json::Error> {
        let raw: RawFilter = serde_json::from_value(Value::Object(map))?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawFilter) -> Result<Self, serde_json::Error> {
        let mut filter = Filter {
            ids: raw.ids,
            authors: raw.authors,
            kinds: raw.kinds,
            since: raw.since,
            until: raw.until,
            limit: raw.limit,
            search: raw.search,
            generic_tags: Vec::new(),
        };

        for (key, val) in raw.extra {
            if !key.starts_with('#') {
                return Err(DeError::custom(format!("unsupported filter field: {key}")));
            }
            if key.len() != 2 {
                return Err(DeError::custom(format!("invalid tag identifier: {key}")));
            }
            let mut chars = key.chars();
            let _ = chars.next(); // '#'
            let tag_char = chars.next().expect("validated length");
            if !tag_char.is_ascii_alphabetic() {
                return Err(DeError::custom(format!("invalid tag identifier: {key}")));
            }
            let values = match val {
                Value::Array(items) => items
                    .into_iter()
                    .map(|item| match item {
                        Value::String(s) => Ok(s),
                        other => Err(DeError::custom(format!(
                            "tag values must be strings, got {other:?}"
                        ))),
                    })
                    .collect::<Result<Vec<String>, _>>()?,
                Value::String(s) => vec![s],
                other => {
                    return Err(DeError::custom(format!(
                        "tag values must be string array, got {other:?}"
                    )));
                }
            };
            if values.is_empty() {
                continue;
            }
            if let Some((_, existing)) = filter
                .generic_tags
                .iter_mut()
                .find(|(candidate, _)| candidate == &tag_char)
            {
                existing.extend(values);
            } else {
                filter.generic_tags.push((tag_char, values));
            }
        }

        Ok(filter)
    }

    fn to_raw(&self) -> RawFilter {
        let mut extra = BTreeMap::new();
        for (tag, values) in &self.generic_tags {
            let key = format!("#{tag}");
            let array = values
                .iter()
                .cloned()
                .map(Value::String)
                .collect::<Vec<Value>>();
            extra.insert(key, Value::Array(array));
        }

        RawFilter {
            ids: self.ids.clone(),
            authors: self.authors.clone(),
            kinds: self.kinds.clone(),
            since: self.since,
            until: self.until,
            limit: self.limit,
            search: self.search.clone(),
            extra,
        }
    }
}

impl Serialize for Filter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_raw().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawFilter::deserialize(deserializer)?;
        Self::from_raw(raw).map_err(|err| D::Error::custom(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::Filter;
    use serde_json::json;

    #[test]
    fn rejects_unknown_property_in_object() {
        let value = json!({
            "ids": [],
            "foo": "bar"
        });

        let err = Filter::from_value(value).unwrap_err();
        assert_eq!(err.to_string(), "unsupported filter field: foo");
    }

    #[test]
    fn rejects_unknown_property_in_array() {
        let value = json!([
            { "authors": [] },
            { "bar": [] }
        ]);

        let err = Filter::from_value(value).unwrap_err();
        assert_eq!(err.to_string(), "unsupported filter field: bar");
    }
}
