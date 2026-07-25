use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use rand::RngCore;
use serde_cbor::Value;

use crate::constants::*;

pub type Map = BTreeMap<Value, Value>;

#[derive(Clone, Debug, PartialEq)]
pub struct Envelope {
    pub fields: Map,
}

fn key(value: i128) -> Value {
    Value::Integer(value)
}

impl Envelope {
    pub fn new(message_type: u64, source: &[u8]) -> Self {
        let mut id = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut id);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i128;
        let mut fields = Map::new();
        fields.insert(key(K_V), Value::Integer(VERSION as i128));
        fields.insert(key(K_T), Value::Integer(message_type as i128));
        fields.insert(key(K_ID), Value::Bytes(id.to_vec()));
        fields.insert(key(K_TS), Value::Integer(timestamp));
        fields.insert(key(K_SRC), Value::Bytes(source.to_vec()));
        Self { fields }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_cbor::from_slice(bytes).context("invalid CBOR")?;
        let Value::Map(fields) = value else {
            bail!("envelope must be a CBOR map");
        };
        let envelope = Self { fields };
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_cbor::to_vec(&Value::Map(self.fields.clone())).context("CBOR encoding failed")
    }

    pub fn validate(&self) -> Result<()> {
        for field in self.fields.keys() {
            if !matches!(field, Value::Integer(value) if *value >= 0) {
                bail!("envelope keys must be unsigned integers");
            }
        }
        for required in [K_V, K_T, K_ID, K_TS, K_SRC] {
            if !self.fields.contains_key(&key(required)) {
                bail!("missing envelope key {required}");
            }
        }
        if self.integer(K_V) != Some(VERSION) {
            bail!("unsupported protocol version");
        }
        if self.integer(K_T).is_none() {
            bail!("message type must be an unsigned integer");
        }
        if self.bytes(K_ID).is_none() {
            bail!("message id must be bytes");
        }
        if self.unsigned(K_TS).is_none() {
            bail!("timestamp must be unsigned");
        }
        if self.bytes(K_SRC).is_none() {
            bail!("sender identity must be bytes");
        }
        if self
            .get(K_ROOM)
            .is_some_and(|v| !matches!(v, Value::Text(_)))
        {
            bail!("room name must be a string");
        }
        if self
            .get(K_NICK)
            .is_some_and(|v| !matches!(v, Value::Text(_)))
        {
            bail!("nickname must be a string");
        }
        if self
            .get(K_DST)
            .is_some_and(|v| !matches!(v, Value::Bytes(_)))
        {
            bail!("destination identity must be bytes");
        }
        Ok(())
    }

    pub fn get(&self, field: i128) -> Option<&Value> {
        self.fields.get(&key(field))
    }
    pub fn integer(&self, field: i128) -> Option<u64> {
        self.unsigned(field).and_then(|v| u64::try_from(v).ok())
    }
    pub fn unsigned(&self, field: i128) -> Option<u128> {
        match self.get(field)? {
            Value::Integer(v) if *v >= 0 => Some(*v as u128),
            _ => None,
        }
    }
    pub fn text(&self, field: i128) -> Option<&str> {
        match self.get(field)? {
            Value::Text(v) => Some(v),
            _ => None,
        }
    }
    pub fn bytes(&self, field: i128) -> Option<&[u8]> {
        match self.get(field)? {
            Value::Bytes(v) => Some(v),
            _ => None,
        }
    }
    pub fn map(&self, field: i128) -> Option<&Map> {
        match self.get(field)? {
            Value::Map(v) => Some(v),
            _ => None,
        }
    }
    pub fn set(&mut self, field: i128, value: Value) {
        self.fields.insert(key(field), value);
    }
    pub fn remove(&mut self, field: i128) {
        self.fields.remove(&key(field));
    }
}

pub fn normalize_nick(value: &str, max_bytes: usize) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > max_bytes || value.contains(['\n', '\r', '\0']) {
        None
    } else {
        Some(value.to_string())
    }
}

pub fn map_get(map: &Map, field: i128) -> Option<&Value> {
    map.get(&key(field))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trip_uses_integer_keys() {
        let mut envelope = Envelope::new(T_MSG, &[7; 16]);
        envelope.set(K_ROOM, Value::Text("test".into()));
        envelope.set(K_BODY, Value::Text("hello".into()));
        let decoded = Envelope::decode(&envelope.encode().unwrap()).unwrap();
        assert_eq!(decoded.integer(K_T), Some(T_MSG));
        assert_eq!(decoded.text(K_ROOM), Some("test"));
    }

    #[test]
    fn rejects_missing_required_fields() {
        let bytes = serde_cbor::to_vec(&Value::Map(Map::new())).unwrap();
        assert!(Envelope::decode(&bytes).is_err());
    }

    #[test]
    fn rejects_non_integer_and_negative_keys_but_allows_extensions() {
        let mut envelope = Envelope::new(T_HELLO, &[7; 16]);
        envelope
            .fields
            .insert(Value::Text("bad".into()), Value::Bool(true));
        assert!(envelope.encode().is_err());

        let mut envelope = Envelope::new(T_HELLO, &[7; 16]);
        envelope
            .fields
            .insert(Value::Integer(-1), Value::Bool(true));
        assert!(envelope.encode().is_err());

        let mut envelope = Envelope::new(T_HELLO, &[7; 16]);
        envelope
            .fields
            .insert(Value::Integer(64), Value::Bool(true));
        assert!(envelope.encode().is_ok());
    }
}
