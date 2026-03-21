package nbd

import "context"

// ExportProvider resolves exports dynamically by name.
// This is the dynamic counterpart to passing a static []Export to Serve.
type ExportProvider interface {
	// FindExport returns the export for the given name.
	// If name is empty, return a default export.
	FindExport(ctx context.Context, name string) (Export, error)

	// ListExports returns all available export names.
	// The Device field may be nil in the returned exports (only Name is required for listing).
	ListExports(ctx context.Context) ([]Export, error)
}
