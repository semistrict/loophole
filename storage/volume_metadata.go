package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/semistrict/loophole/objstore"
)

// ListAllVolumes returns the names of all volumes in the store.
func ListAllVolumes(ctx context.Context, store objstore.ObjectStore) ([]string, error) {
	volRefs := store.At("volumes")
	objects, err := volRefs.ListKeys(ctx, "")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, obj := range objects {
		name, _, ok := strings.Cut(obj.Key, "/")
		if ok && name != "" && ValidateVolumeName(name) == nil {
			seen[name] = true
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func GetVolumeInfo(ctx context.Context, store objstore.ObjectStore, name string) (VolumeInfo, error) {
	if err := ValidateVolumeName(name); err != nil {
		return VolumeInfo{}, err
	}
	ref, err := getVolumeRef(ctx, store.At("volumes"), name)
	if err != nil {
		return VolumeInfo{}, err
	}
	return VolumeInfo{
		Name:   name,
		Size:   ref.Size,
		Type:   ref.Type,
		Parent: ref.Parent,
		Labels: ref.Labels,
	}, nil
}

func UpdateLabels(ctx context.Context, store objstore.ObjectStore, name string, labels map[string]string) error {
	volRefs := store.At("volumes")
	key, err := volumeIndexKey(name)
	if err != nil {
		return err
	}
	return objstore.ModifyJSON[volumeRef](ctx, volRefs, key, func(ref *volumeRef) error {
		ref.Labels = labels
		return nil
	})
}

func DeleteVolume(ctx context.Context, store objstore.ObjectStore, name string) error {
	if err := ValidateVolumeName(name); err != nil {
		return err
	}

	volRefs := store.At("volumes")
	v := &Volume{
		name:  name,
		lease: objstore.NewLeaseSession(store.At("leases")),
		manager: &Manager{
			store:   store,
			volRefs: volRefs,
		},
	}
	if _, err := v.acquireLease(ctx); err != nil {
		return err
	}
	defer v.closeLeaseSession(ctx)
	defer v.releaseLease(ctx)

	cpPrefix, err := checkpointPrefix(name)
	if err != nil {
		return err
	}
	cpKeys, _ := volRefs.ListKeys(ctx, cpPrefix)
	for _, obj := range cpKeys {
		_ = volRefs.DeleteObject(ctx, obj.Key)
	}
	key, err := volumeIndexKey(name)
	if err != nil {
		return err
	}
	if err := volRefs.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("delete volume ref %q: %w", name, err)
	}
	return nil
}
