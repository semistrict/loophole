//! Typed inode system for multi-volume FUSE/NFS.
//!
//! Three inode domains exist:
//!
//! - **`Ext4Ino`**: Raw inode from lwext4. Must be remapped with a `VolIdx`
//!   before being exposed to FUSE/NFS.
//! - **`VirtualIno`**: Synthetic inode for the `.loophole/` control tree.
//!   Lives in the high end of the u64 space (>= `VIRTUAL_INO_START`).
//! - **`GlobalIno`**: The unified inode type safe to hand to FUSE/NFS clients.
//!   Constructed only through explicit conversion from one of the above.
//!
//! Layout of the u64 inode space:
//! ```text
//!   0                    reserved (invalid)
//!   1                    ROOT (virtual root of the mount)
//!   2..INODE_OFFSET-1    unused
//!   INODE_OFFSET ..      remapped ext4 inodes: (vol_idx+1)*OFFSET + ext4_ino
//!   ...
//!   VIRTUAL_INO_START..  synthetic ctl inodes (well-known + allocated downward)
//! ```

/// Each volume's ext4 inodes are offset by this amount.
/// ext4 inodes are 32-bit, so 2^40 provides ample headroom.
const INODE_OFFSET: u64 = 1 << 40;

/// Virtual/ctl inodes live in [VIRTUAL_INO_START, u64::MAX].
/// We reserve 1 million slots — far more than needed (a few hundred at most)
/// but still well above the highest possible remapped ext4 inode.
pub const VIRTUAL_INO_START: u64 = u64::MAX - 1_000_000;

// ---------------------------------------------------------------------------
// Well-known virtual inode constants
// ---------------------------------------------------------------------------

/// `.loophole/` control directory.
pub const CTL_DIR_INO: VirtualIno = VirtualIno::new_const(VIRTUAL_INO_START);
/// `.loophole/snapshots/` directory.
pub const SNAPSHOTS_DIR_INO: VirtualIno = VirtualIno::new_const(VIRTUAL_INO_START + 1);
/// `.loophole/clones/` directory.
pub const CLONES_DIR_INO: VirtualIno = VirtualIno::new_const(VIRTUAL_INO_START + 2);

/// Dynamic virtual inode allocator starts here and counts downward from u64::MAX.
/// Well-known constants occupy [VIRTUAL_INO_START .. VIRTUAL_INO_START+10),
/// so dynamic allocation starts at the top and grows down, never colliding.
pub const ALLOC_START: u64 = u64::MAX;

// ---------------------------------------------------------------------------
// VolIdx
// ---------------------------------------------------------------------------

/// Index of a volume in the multi-volume registry. Max 65535 volumes.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct VolIdx(u16);

impl VolIdx {
    pub fn new(idx: u16) -> Self {
        Self(idx)
    }

    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

// ---------------------------------------------------------------------------
// Ext4Ino
// ---------------------------------------------------------------------------

/// Raw inode number from lwext4. Must not be sent to FUSE/NFS directly.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Ext4Ino(u64);

impl Ext4Ino {
    /// Wrap a raw inode from lwext4.
    /// Panics if the value doesn't fit in the per-volume inode space.
    pub fn new(raw: u64) -> Self {
        assert!(
            raw < INODE_OFFSET,
            "ext4 inode {raw} exceeds max {INODE_OFFSET} — filesystem corrupt?"
        );
        Self(raw)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// VirtualIno
// ---------------------------------------------------------------------------

/// Synthetic inode for the `.loophole/` control directory tree.
/// Lives in [VIRTUAL_INO_START, u64::MAX].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct VirtualIno(u64);

impl VirtualIno {
    /// Wrap a value that must already be in the virtual range.
    /// Panics otherwise.
    pub fn new(raw: u64) -> Self {
        assert!(
            raw >= VIRTUAL_INO_START,
            "not a virtual inode: {raw} < {VIRTUAL_INO_START}"
        );
        Self(raw)
    }

    /// Const-compatible constructor for well-known inode constants.
    /// The caller must guarantee `raw >= VIRTUAL_INO_START`.
    /// (The `well_known_constants_in_range` test verifies this.)
    pub const fn new_const(raw: u64) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// GlobalIno
// ---------------------------------------------------------------------------

/// Inode in the global FUSE/NFS namespace. Safe to return to clients.
///
/// Constructed only through:
/// - `GlobalIno::ROOT`
/// - `GlobalIno::from_ext4(vol, ext4_ino)`
/// - `GlobalIno::from_virtual(virt_ino)`
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct GlobalIno(u64);

impl GlobalIno {
    /// The root inode of the FUSE/NFS mount.
    pub const ROOT: Self = Self(1);

    /// Remap an ext4 inode into the global namespace for a specific volume.
    pub fn from_ext4(vol: VolIdx, ext4: Ext4Ino) -> Self {
        Self((vol.0 as u64 + 1) * INODE_OFFSET + ext4.0)
    }

    /// Wrap a virtual/ctl inode.
    pub fn from_virtual(virt: VirtualIno) -> Self {
        Self(virt.0)
    }

    /// Wrap an already-global inode from FUSE/NFS (e.g. from `INodeNo`).
    /// Use when receiving inodes from the kernel that were previously vended by us.
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw u64 value, for passing to fuser/nfsserve APIs.
    pub fn raw(self) -> u64 {
        self.0
    }

    /// True if this is the virtual root inode.
    pub fn is_root(self) -> bool {
        self.0 == 1
    }

    /// True if this is a virtual/ctl inode.
    pub fn is_virtual(self) -> bool {
        self.0 >= VIRTUAL_INO_START
    }

    /// Decompose into volume index + ext4 inode.
    /// Returns `None` for root and virtual inodes.
    pub fn to_ext4(self) -> Option<(VolIdx, Ext4Ino)> {
        if self.0 < INODE_OFFSET || self.0 >= VIRTUAL_INO_START {
            return None;
        }
        let slot = self.0 / INODE_OFFSET;
        let ext4_ino = self.0 % INODE_OFFSET;
        let vol_idx = u16::try_from(slot - 1).ok()?;
        Some((VolIdx(vol_idx), Ext4Ino(ext4_ino)))
    }

    /// Decompose into a VirtualIno. Returns `None` if not virtual.
    pub fn to_virtual(self) -> Option<VirtualIno> {
        if self.0 >= VIRTUAL_INO_START {
            Some(VirtualIno(self.0))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_roundtrip() {
        assert!(GlobalIno::ROOT.is_root());
        assert!(!GlobalIno::ROOT.is_virtual());
        assert_eq!(GlobalIno::ROOT.to_ext4(), None);
    }

    #[test]
    fn ext4_roundtrip() {
        let vol = VolIdx::new(0);
        let ext4 = Ext4Ino::new(2); // ext4 root
        let global = GlobalIno::from_ext4(vol, ext4);

        assert!(!global.is_root());
        assert!(!global.is_virtual());

        let (v, e) = global.to_ext4().unwrap();
        assert_eq!(v, vol);
        assert_eq!(e, ext4);
    }

    #[test]
    fn ext4_different_volumes_no_collision() {
        let ext4 = Ext4Ino::new(42);
        let g0 = GlobalIno::from_ext4(VolIdx::new(0), ext4);
        let g1 = GlobalIno::from_ext4(VolIdx::new(1), ext4);
        assert_ne!(g0, g1);

        let (v0, e0) = g0.to_ext4().unwrap();
        let (v1, e1) = g1.to_ext4().unwrap();
        assert_eq!(v0, VolIdx::new(0));
        assert_eq!(v1, VolIdx::new(1));
        assert_eq!(e0, e1);
    }

    #[test]
    fn virtual_roundtrip() {
        let virt = VirtualIno::new(VIRTUAL_INO_START);
        let global = GlobalIno::from_virtual(virt);

        assert!(global.is_virtual());
        assert!(!global.is_root());
        assert_eq!(global.to_ext4(), None);
        assert_eq!(global.to_virtual().unwrap(), virt);
    }

    #[test]
    #[should_panic(expected = "ext4 inode")]
    fn ext4_ino_too_large_panics() {
        Ext4Ino::new(INODE_OFFSET);
    }

    #[test]
    #[should_panic(expected = "not a virtual inode")]
    fn virtual_ino_too_small_panics() {
        VirtualIno::new(42);
    }

    #[test]
    fn no_overlap_between_ext4_and_virtual() {
        // Highest possible ext4 global inode (vol=65535, ext4_ino=2^40-1)
        let max_ext4 = GlobalIno::from_ext4(VolIdx::new(u16::MAX), Ext4Ino::new(INODE_OFFSET - 1));
        // Lowest virtual inode
        let min_virtual = GlobalIno::from_virtual(VirtualIno::new(VIRTUAL_INO_START));

        assert!(max_ext4.raw() < min_virtual.raw());
    }

    #[test]
    fn well_known_constants_in_range() {
        // Verify the well-known constants are valid VirtualInos.
        // They use new_const() which skips the runtime assert, so we check here.
        assert!(CTL_DIR_INO.raw() >= VIRTUAL_INO_START);
        assert!(SNAPSHOTS_DIR_INO.raw() >= VIRTUAL_INO_START);
        assert!(CLONES_DIR_INO.raw() >= VIRTUAL_INO_START);
    }
}
