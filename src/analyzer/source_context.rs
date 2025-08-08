use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{ScalarValue, TableReference};

/// The context of a column reference's source. Column references identified throughout a plan tree,
/// if resolveable to table's at the leafs of the plan, can be "grouped" by their source context.
/// Otherwise the `TableReference` is tracked directly.
///
/// Examples of usage is for testing, via `grouped_sources`, or in the context of a
/// `ClickHouseTableProvider`, the context returned by its `unique_context`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SourceContext {
    /// The unique context of a `ClickHouse` table
    Context(String),
    /// The `TableReference` of any table
    Table(TableReference),
    /// A Scalar value
    Scalar(ScalarValue),
    /// A `LogicalPlan::Values`
    Values,
}

impl PartialOrd for SourceContext {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for SourceContext {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SourceContext::Context(a), SourceContext::Context(b)) => a.cmp(b),
            (SourceContext::Table(a), SourceContext::Table(b)) => a.cmp(b),
            (SourceContext::Values, SourceContext::Values) => std::cmp::Ordering::Equal,
            (a, b) => {
                // If both source context, attempt compare
                if let (SourceContext::Scalar(a), SourceContext::Scalar(b)) = (a, b)
                    && let Some(ordering) = a.partial_cmp(b)
                {
                    return ordering;
                }

                // Otherwise hash a and b and order deterministically
                let hash_self = {
                    let mut hasher = DefaultHasher::new();
                    a.hash(&mut hasher);
                    hasher.finish()
                };

                let hash_other = {
                    let mut hasher = DefaultHasher::new();
                    b.hash(&mut hasher);
                    hasher.finish()
                };

                hash_self.cmp(&hash_other)
            }
        }
    }
}

/// This structure is used to control the `SourceContext`s when grouped together. There are a number
/// of invariants that need to hold, for example `Vec<Arc<SourceContext>>` ==
/// `Vec<Arc<SourceContext>>` if there is a 1:1 mapping between the two, regardless of order. A
/// `HashSet` could be used but it makes it difficult to operate on it like a `Vec`, allowing any
/// containers to derive `Hash` for instance. This structure allows controlled access and updates.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Default)]
pub(crate) struct SourceContextSet(Vec<Arc<SourceContext>>);

impl SourceContextSet {
    pub(crate) fn new(mut contexts: Vec<Arc<SourceContext>>) -> Self {
        contexts.sort_unstable();
        contexts.dedup();
        Self(contexts)
    }

    pub(crate) fn push(&mut self, context: Arc<SourceContext>) {
        if let Err(pos) = self.0.binary_search(&context) {
            self.0.insert(pos, context);
        }
    }

    pub(crate) fn extend(&mut self, contexts: impl IntoIterator<Item = Arc<SourceContext>>) {
        self.0.extend(contexts);
        self.0.sort_unstable();
        self.0.dedup();
    }
}

impl std::ops::Deref for SourceContextSet {
    type Target = [Arc<SourceContext>];

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl From<Vec<Arc<SourceContext>>> for SourceContextSet {
    fn from(contexts: Vec<Arc<SourceContext>>) -> Self { SourceContextSet::new(contexts) }
}

impl IntoIterator for SourceContextSet {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = Arc<SourceContext>;

    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}

/// Derived from a column ref's `ColumnLineage`. Identifies how a column reference resolves to its
/// source, whether a column in a `TableScan` or a scalar/literal value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) enum ResolvedSource {
    /// Single column from single table
    Exact(Arc<SourceContext>),
    /// Multiple columns all from same table
    Simple(Arc<SourceContext>),
    /// Multiple columns from multiple tables
    Compound(SourceContextSet),
    /// Scalar/literal value
    Scalar(Arc<SourceContext>),
    Scalars(SourceContextSet), // Only constructed during merge
    /// Unknown/unresolved column - no lineage information available
    #[default]
    Unknown,
}

impl ResolvedSource {
    pub(crate) fn is_known(&self) -> bool { !matches!(self, ResolvedSource::Unknown) }

    /// An equality-like check to determine if `self` is fully resolved by the sources of `other`
    pub(crate) fn resolves_eq(&self, target: &ResolvedSource) -> bool {
        let Some(restricting_contexts) = self.contexts() else {
            return false;
        };
        let Some(target_contexts) = target.contexts() else {
            return false;
        };

        // Find if there are any source contexts defined in target that is not in self
        target_contexts.symmetric_difference(&restricting_contexts).copied().count() == 0
    }

    /// An equality-like check to determine if `self` is fully resolved by the sources of `other`
    pub(crate) fn resolves_intersects(&self, other: &ResolvedSource) -> bool {
        let Some(self_contexts) = self.contexts() else {
            return false;
        };
        let Some(other_contexts) = other.contexts() else {
            return false;
        };
        self_contexts.intersection(&other_contexts).next().is_some()
    }

    /// An equality-like check to determine if `self` is fully resolved by the sources of `other`
    pub(crate) fn resolves_within(&self, other: &ResolvedSource) -> bool {
        let Some(self_contexts) = self.contexts() else {
            return false;
        };
        let Some(other_contexts) = other.contexts() else {
            return false;
        };
        self_contexts.is_subset(&other_contexts)
    }

    /// Gather the source contexts (or table references) of this resolved source (column reference)
    ///
    /// NOTE: Important! Ensure `Scalar` and `Scalars` are not included in the source's contexts as
    /// they should not block pushdown
    pub(crate) fn contexts(&self) -> Option<HashSet<&SourceContext>> {
        Some(match self {
            ResolvedSource::Simple(table) | ResolvedSource::Exact(table) => {
                HashSet::from([table.as_ref()])
            }
            ResolvedSource::Compound(sources) => sources
                .iter()
                // Do not include scalar sources as they should not block pushdown
                .filter(|s| !matches!(s.as_ref(), SourceContext::Scalar(_)))
                .map(AsRef::as_ref)
                .collect(),
            ResolvedSource::Scalar(_) | ResolvedSource::Scalars(_) => HashSet::new(),
            ResolvedSource::Unknown => return None,
        })
    }

    /// Merge this source with another, taking ownership to avoid clones
    ///
    /// NOTE: Important! Ensure `Scalar` and `Scalars` are not included in the source's contexts as
    /// they should not block pushdown
    pub(crate) fn merge(self, other: ResolvedSource) -> ResolvedSource {
        #[allow(clippy::enum_glob_use)]
        use ResolvedSource::*;

        match (self, other) {
            // Exact + Exact
            (Exact(t1), Exact(t2)) => {
                if t1 == t2 {
                    Exact(t1)
                } else {
                    Compound(vec![t1, t2].into())
                }
            }
            // Exact + Simple
            (Exact(t1), Simple(t2)) | (Simple(t1), Exact(t2)) => {
                if t1 == t2 {
                    Simple(t2)
                } else {
                    Compound(vec![t1, t2].into())
                }
            }
            // Simple + Simple
            (Simple(t1), Simple(t2)) => {
                if t1 == t2 {
                    Simple(t1)
                } else {
                    Compound(vec![t1, t2].into())
                }
            }
            // Unknown + anything = if other is not Unknown, return other, otherwise return Unknown
            (Unknown, other) | (other, Unknown) => other,
            // Scalars are handled a bit special. They shouldn't block pushdown, so only account for
            // when it's definitely a scalar
            (Scalar(s1), Scalar(s2)) => {
                if s1 == s2 {
                    Scalar(s1)
                } else {
                    Scalars(vec![s1, s2].into())
                }
            }
            (Scalars(mut s1), Scalars(s2)) => {
                s1.extend(s2);
                Scalars(s1)
            }
            (Scalars(mut s), Scalar(s1)) | (Scalar(s1), Scalars(mut s)) => {
                s.push(s1);
                Scalars(s)
            }
            (Scalar(s1), Exact(t1) | Simple(t1)) | (Exact(t1) | Simple(t1), Scalar(s1)) => {
                Compound(vec![s1, t1].into())
            }
            (Exact(t) | Simple(t), Scalars(mut s)) | (Scalars(mut s), Exact(t) | Simple(t)) => {
                s.push(t);
                Compound(s)
            }

            // Any + Compound or Compound + Any
            (source, Compound(mut sources)) | (Compound(mut sources), source) => {
                match source {
                    Exact(table) | Simple(table) | Scalar(table) => sources.push(table),
                    Compound(other_sources) | Scalars(other_sources) => {
                        sources.extend(other_sources);
                    }
                    // Already handled above
                    Unknown => {}
                }
                Compound(sources)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_source_is_known() {
        assert!(!ResolvedSource::Unknown.is_known());
        assert!(
            ResolvedSource::Scalar(Arc::new(SourceContext::Scalar(ScalarValue::Int32(Some(42)))))
                .is_known()
        );
        assert!(ResolvedSource::Exact(Arc::new(SourceContext::Values)).is_known());
        assert!(ResolvedSource::Simple(Arc::new(SourceContext::Values)).is_known());
        assert!(ResolvedSource::Compound(vec![Arc::new(SourceContext::Values)].into()).is_known());
    }

    #[test]
    fn test_resolved_source_resolves_eq() {
        let source1 = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let source2 = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let source3 =
            ResolvedSource::Exact(Arc::new(SourceContext::Table(TableReference::bare("other"))));

        assert!(source1.resolves_eq(&source2));
        assert!(!source1.resolves_eq(&source3));
        assert!(!ResolvedSource::Unknown.resolves_eq(&source1));
    }

    #[test]
    fn test_resolved_source_resolves_contains() {
        let source1 = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let source2 = ResolvedSource::Compound(
            vec![
                Arc::new(SourceContext::Values),
                Arc::new(SourceContext::Table(TableReference::bare("test"))),
            ]
            .into(),
        );

        assert!(source1.resolves_intersects(&source2));
        assert!(!ResolvedSource::Unknown.resolves_intersects(&source1));
    }

    #[test]
    fn test_resolved_source_contexts() {
        let source = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let contexts = source.contexts();
        assert!(contexts.is_some());
        assert_eq!(contexts.unwrap().len(), 1);

        assert!(ResolvedSource::Unknown.contexts().is_none());

        let scalar_source =
            ResolvedSource::Scalar(Arc::new(SourceContext::Scalar(ScalarValue::Int32(Some(42)))));
        let scalar_contexts = scalar_source.contexts();
        assert!(scalar_contexts.is_some());
        assert!(scalar_contexts.unwrap().is_empty());
    }

    #[test]
    fn test_resolved_source_merge_exact_exact() {
        let source1 = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let source2 = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let merged = source1.merge(source2);

        match merged {
            ResolvedSource::Exact(_) => {} // Same sources merge to Exact
            _ => panic!("Expected Exact result from merging identical sources"),
        }

        let source3 =
            ResolvedSource::Exact(Arc::new(SourceContext::Table(TableReference::bare("other"))));
        let source4 = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let merged2 = source3.merge(source4);

        match merged2 {
            ResolvedSource::Compound(_) => {} // Different sources merge to Compound
            _ => panic!("Expected Compound result from merging different sources"),
        }
    }

    #[test]
    fn test_resolved_source_merge_with_unknown() {
        let source = ResolvedSource::Exact(Arc::new(SourceContext::Values));
        let unknown = ResolvedSource::Unknown;

        let merged1 = source.clone().merge(unknown.clone());
        let merged2 = unknown.merge(source);

        match (merged1, merged2) {
            (ResolvedSource::Exact(_), ResolvedSource::Exact(_)) => {} // Unknown should be ignored
            _ => panic!("Expected non-Unknown to be preserved when merging with Unknown"),
        }
    }

    #[test]
    fn test_source_context_variants() {
        let context1 = SourceContext::Context("test_context".to_string());
        let context2 = SourceContext::Table(TableReference::bare("test_table"));
        let context3 = SourceContext::Scalar(ScalarValue::Null);
        let context4 = SourceContext::Values;

        match &context1 {
            SourceContext::Context(s) => assert_eq!(s, "test_context"),
            _ => panic!("Expected Context variant"),
        }

        match &context2 {
            SourceContext::Table(table_ref) => assert_eq!(table_ref.table(), "test_table"),
            _ => panic!("Expected Table variant"),
        }

        match &context3 {
            SourceContext::Scalar(s) => assert_eq!(s, &ScalarValue::Null),
            _ => panic!("Expected Values variant"),
        }

        match &context4 {
            SourceContext::Values => {} // Expected
            _ => panic!("Expected Values variant"),
        }

        assert_ne!(context1, context2);
        assert_ne!(context2, context3);
        assert_ne!(context3, context4);
    }

    #[test]
    fn test_resolved_sources_merge() {
        let scalar1 = SourceContext::Scalar(ScalarValue::Utf8(Some("hello".into())));
        let scalar2 = SourceContext::Scalar(ScalarValue::Int32(Some(0)));
        let resolved1 = ResolvedSource::Scalar(scalar1.clone().into());
        let resolved2 = ResolvedSource::Scalar(scalar2.clone().into());
        let resolved3 = ResolvedSource::Scalars(vec![scalar1.clone().into()].into());
        let resolved4 = ResolvedSource::Scalars(vec![scalar2.clone().into()].into());
        let resolved5 = ResolvedSource::Simple(scalar1.clone().into());
        let resolved6 = ResolvedSource::Simple(scalar2.clone().into());
        let resolved6b = ResolvedSource::Simple(scalar2.clone().into());
        let resolved7 = ResolvedSource::Exact(scalar1.clone().into());

        let merged_simple = resolved5.merge(resolved6.clone());
        assert_eq!(
            merged_simple,
            ResolvedSource::Compound(vec![scalar1.clone().into(), scalar2.clone().into()].into()),
            "Simple with different table merges to Compound"
        );
        let merged_simple = resolved6.merge(resolved6b.clone());
        assert_eq!(
            merged_simple,
            ResolvedSource::Simple(scalar2.clone().into()),
            "Simple with same table merges to Simple"
        );
        let merged_simple_exact = resolved6b.merge(resolved7);
        assert_eq!(
            merged_simple_exact,
            ResolvedSource::Compound(vec![scalar1.clone().into(), scalar2.clone().into()].into()),
            "Simple with same table (Exact) merges to Simple"
        );

        let merged_resolved_scalar = resolved1.clone().merge(resolved2.clone());
        let merged_resolved_scalar_set = resolved3.clone().merge(resolved4.clone());

        assert_eq!(
            merged_resolved_scalar, merged_resolved_scalar_set,
            "Merging produces the same set"
        );

        // De-dupes
        let merged_deduped = resolved3.clone().merge(resolved1.clone());
        assert_eq!(&merged_deduped, &resolved3, "Merging produces the same set");
    }

    // Ensure scalars do not disrupt pushdown resolution
    #[test]
    fn test_resolved_source_merge_with_scalar() {
        let source_table = Arc::new(SourceContext::Context("test_table".to_string()));
        let source = ResolvedSource::Exact(Arc::clone(&source_table));
        let scalar =
            ResolvedSource::Scalar(Arc::new(SourceContext::Scalar(ScalarValue::Int32(Some(42)))));

        let merged1 = source.clone().merge(scalar.clone());
        let merged2 = scalar.merge(source);

        match (&merged1, &merged2) {
            (ResolvedSource::Compound(_), ResolvedSource::Compound(_)) => {}
            _ => panic!("Expected non-Scalar to be preserved when merging with Scalar"),
        }

        /* Scalar should be ignored */
        let other_source = ResolvedSource::Exact(source_table);
        let other_ok = other_source.resolves_eq(&merged1);
        let compound_ok = merged1.resolves_eq(&other_source);
        assert!(other_ok, "Expected resolving to ignore scalars");
        assert_eq!(other_ok, compound_ok, "Expected resolving to be symmetric");
    }
}
