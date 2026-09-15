use super::TableBuilder;
use crate::{MemoryBudget, Result, Value};

impl TableBuilder {
    pub(crate) fn capacity_bytes(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>()
            + self.columns.capacity() * std::mem::size_of::<crate::columnar::ColumnBuilder>()
            + self
                .columns
                .iter()
                .map(|c| c.capacity_bytes())
                .sum::<usize>()
            + self.column_order.capacity() * std::mem::size_of::<String>()
            + self
                .column_order
                .iter()
                .map(String::capacity)
                .sum::<usize>()
            + self.field_index.capacity() * (std::mem::size_of::<(String, usize)>() + 1)
            + self.field_index.keys().map(String::capacity).sum::<usize>()
            + self.row_dirty.capacity() * 8
            + self.ordinal_expect.capacity() * std::mem::size_of::<Option<(u32, Vec<u8>)>>()
            + self
                .ordinal_expect
                .iter()
                .flatten()
                .map(|(_, name)| name.capacity())
                .sum::<usize>();
        if let Some(buf) = &self.row_buf {
            bytes += std::mem::size_of_val(buf.as_ref());
            if buf.fields.spilled() {
                bytes += buf.fields.capacity() * std::mem::size_of::<(u32, Value<'static>)>();
            }
            bytes += buf
                .fields
                .iter()
                .map(|(_, value)| match value {
                    Value::Str(std::borrow::Cow::Owned(s)) => s.capacity(),
                    _ => 0,
                })
                .sum::<usize>();
            if buf.predicate_mask.spilled() {
                bytes += buf.predicate_mask.capacity() * 8;
            }
            if buf.pred_names.spilled() {
                bytes += buf.pred_names.capacity() * std::mem::size_of::<String>();
            }
            bytes += buf.pred_names.iter().map(String::capacity).sum::<usize>();
        }
        bytes
    }

    pub(crate) fn set_memory_budget(&mut self, budget: MemoryBudget) -> Result<()> {
        self.memory_limit = budget.is_strict().then_some(budget.bytes());
        self.check_memory_budget()
    }

    pub(crate) fn check_memory_budget(&self) -> Result<()> {
        if let Some(crate::Error::Memory { used, limit }) = self.strict_error.as_ref() {
            return Err(crate::Error::Memory {
                used: *used,
                limit: *limit,
            });
        }
        if let Some(limit) = self.memory_limit {
            let used = self.capacity_bytes();
            if used > limit {
                return Err(crate::Error::Memory { used, limit });
            }
        }
        Ok(())
    }

    pub(super) fn reserve_field_memory(&mut self, name_len: usize, value: &Value<'_>) -> bool {
        if self.memory_limit.is_none() {
            return true;
        }
        let value_bytes = value.as_str().map_or(32, str::len);
        self.reserve_memory(value_bytes.saturating_add(name_len).saturating_mul(2))
    }

    pub(super) fn reserve_memory(&mut self, additional: usize) -> bool {
        let Some(limit) = self.memory_limit else {
            return true;
        };
        if matches!(self.strict_error, Some(crate::Error::Memory { .. })) {
            return false;
        }
        let used = self.capacity_bytes().saturating_add(additional);
        if used <= limit {
            return true;
        }
        if self.strict_error.is_none() {
            self.strict_error = Some(crate::Error::Memory { used, limit });
        }
        false
    }
}
