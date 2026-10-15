//! Reusable containers and algorithms independent of AOI roles and events.
//!
//! - [`IdSet`]: contiguous u32 IDs with a lazy index for dense sets.
//! - [`VisitMarks`]: generation-based deduplication for compact ID spaces.
//! - [`rect_difference`]: allocation-free rectangle subtraction.

mod geometry;
mod id_set;
mod visit_marks;

pub use geometry::rect_difference;
pub use id_set::IdSet;
pub use visit_marks::VisitMarks;
