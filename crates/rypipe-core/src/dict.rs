use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::columnar::ColumnBuilder;

/// Immutable seed dictionary built from a sample.
///
/// Shared `Arc` across threads, read-only after construction.
#[derive(Debug, Clone)]
pub struct SeedDict {
    pub values: Vec<u8>,
    pub offsets: Vec<i32>,
    pub index: FxHashMap<Box<str>, i32>,
}

impl SeedDict {
    pub fn new(values: Vec<String>) -> Self {
        let mut data = Vec::new();
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0);
        let mut index = FxHashMap::default();
        for (i, s) in values.into_iter().enumerate() {
            index.insert(s.clone().into_boxed_str(), i as i32);
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as i32);
        }
        Self {
            values: data,
            offsets,
            index,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Column plan from sampling.
#[derive(Debug, Clone)]
pub struct ColumnPlan {
    pub encode_as_dict: bool,
    pub seed: Option<Arc<SeedDict>>,
    pub est_cardinality: usize,
}

/// Remap table for one chunk's codes.
#[derive(Debug, Clone)]
pub struct RemapTable {
    pub map: Vec<i32>,
    pub is_identity: bool,
}

impl RemapTable {
    pub fn identity(len: usize) -> Self {
        Self {
            map: (0..len as i32).collect(),
            is_identity: true,
        }
    }
}

/// Unified dictionary values.
#[derive(Debug, Clone)]
pub struct DictValues {
    pub data: Vec<u8>,
    pub offsets: Vec<i32>,
}

/// Unify per-chunk dictionaries with the seed.
///
/// Works with `ColumnBuilder::Dictionary { codes, dict, index }`.
/// Seed codes are `0..seed.len()`, overflow are `seed.len()..`.
pub(crate) fn unify_dictionaries(
    seed: &SeedDict,
    locals: &[&ColumnBuilder],
) -> (DictValues, Vec<RemapTable>) {
    let mut global_index: FxHashMap<Box<str>, i32> = FxHashMap::default();
    let mut global_data = seed.values.clone();
    let mut global_offsets = seed.offsets.clone();
    let mut next_code = seed.len() as i32;

    for (k, &v) in &seed.index {
        global_index.insert(k.clone(), v);
    }

    // Collect overflow values not in seed
    for col in locals {
        if let ColumnBuilder::Dictionary { dict, .. } = col {
            for s in dict.iter().skip(seed.len()) {
                let k: Box<str> = s.clone().into_boxed_str();
                if !global_index.contains_key(&k) {
                    global_index.insert(k.clone(), next_code);
                    global_data.extend_from_slice(s.as_bytes());
                    global_offsets.push(global_data.len() as i32);
                    next_code += 1;
                }
            }
        }
    }

    let dict_values = DictValues {
        data: global_data,
        offsets: global_offsets,
    };

    // Build remap tables: local code -> global code
    let mut remaps = Vec::with_capacity(locals.len());
    for col in locals {
        if let ColumnBuilder::Dictionary { dict, index, .. } = col {
            let local_len = dict.len();
            let mut map = Vec::with_capacity(local_len);
            let mut is_identity = true;
            for s in dict {
                let k: Box<str> = s.clone().into_boxed_str();
                let g = *global_index.get(&k).unwrap();
                let local = *index.get(k.as_ref()).unwrap();
                if g != local {
                    is_identity = false;
                }
                map.push(g);
            }
            // If no overflow, it's essentially identity if dict == seed
            if dict.len() == seed.len() && is_identity {
                // All seed, identity
            } else if dict.len() > seed.len() {
                is_identity = false;
            }
            remaps.push(RemapTable { map, is_identity });
        } else {
            remaps.push(RemapTable::identity(0));
        }
    }

    (dict_values, remaps)
}

/// Apply remap to codes in place (parallel, L1-resident).
#[inline]
pub fn apply_remap(codes: &mut [Option<i32>], remap: &RemapTable) {
    if remap.is_identity {
        return;
    }
    for v in codes.iter_mut().flatten() {
        debug_assert!((*v as usize) < remap.map.len(), "code out of bounds");
        unsafe {
            *v = *remap.map.get_unchecked(*v as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnBuilder;
    use crate::plan::FieldType;

    fn make_dict(values: Vec<&str>) -> ColumnBuilder {
        let mut b = ColumnBuilder::with_capacity(10, &FieldType::Dictionary);
        for v in values {
            b.push_str(Some(v));
        }
        b
    }

    #[test]
    fn test_unify_no_overflow() {
        let seed = SeedDict::new(vec!["a".into(), "b".into()]);
        let col1 = make_dict(vec!["a", "b", "a"]);
        let col2 = make_dict(vec!["b", "a"]);
        let (dict, remaps) = unify_dictionaries(&seed, &[&col1, &col2]);
        assert_eq!(dict.offsets.len(), 3);
        // Both remaps should be identity or at least map correctly
        assert_eq!(remaps.len(), 2);
    }

    #[test]
    fn test_apply_remap_identity() {
        let mut codes = vec![Some(0), Some(1), Some(0), None];
        let remap = RemapTable::identity(2);
        apply_remap(&mut codes, &remap);
        assert_eq!(codes, vec![Some(0), Some(1), Some(0), None]);
    }
}
