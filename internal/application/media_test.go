package application

import (
	"context"
	"errors"
	"testing"
)

type mediaMemory struct {
	job             PhotoJob
	completed, fail bool
	invalid         bool
	storageError    bool
	uploads         int
	prepareError    error
	ctxCancel       context.CancelFunc
}

func (m *mediaMemory) ClaimPhoto(context.Context) (PhotoJob, bool, error) { return m.job, true, nil }
func (m *mediaMemory) CompletePhoto(context.Context, PhotoJob, PreparedPhoto) (bool, error) {
	m.completed = true
	return true, nil
}
func (m *mediaMemory) FailPhoto(_ context.Context, _ PhotoJob, invalid bool) error {
	m.fail, m.invalid = true, invalid
	return nil
}
func (*mediaMemory) ClaimCleanup(context.Context) (MediaCleanupJob, bool, error) {
	return MediaCleanupJob{}, false, nil
}
func (*mediaMemory) CompleteCleanup(context.Context, MediaCleanupJob, bool) error { return nil }
func (*mediaMemory) Maintenance(context.Context, bool) error                      { return nil }
func (*mediaMemory) Download(context.Context, string) ([]byte, error)             { return []byte{1}, nil }
func (m *mediaMemory) Upload(context.Context, string, []byte) error {
	m.uploads++
	if m.storageError {
		return errors.New("temporary storage outage")
	}
	return nil
}
func (*mediaMemory) Delete(context.Context, string, string) error { return nil }
func (m *mediaMemory) Prepare(context.Context, []byte, string) (PreparedPhoto, error) {
	if m.ctxCancel != nil {
		m.ctxCancel()
	}
	return PreparedPhoto{}, m.prepareError
}

func TestPhotoPublicationFailureBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		invalid, outage, cancelled bool
	}{
		{"valid", false, false, false}, {"invalid content", true, false, false},
		{"Storage outage", false, true, false}, {"shutdown after preparation", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			m := &mediaMemory{job: PhotoJob{DeclaredBytes: 1}, storageError: tc.outage}
			if tc.invalid {
				m.prepareError = ErrInvalidPhoto
			}
			if tc.cancelled {
				m.ctxCancel = cancel
			}
			worked, err := (PhotoProcessor{Repository: m, Storage: m, Preparer: m}).Once(ctx)
			if !worked || (err != nil) != (tc.outage || tc.cancelled) {
				t.Fatalf("worked=%v err=%v", worked, err)
			}
			if m.completed != (!tc.invalid && !tc.outage && !tc.cancelled) || m.invalid != tc.invalid {
				t.Fatal("incorrect publication/terminal result")
			}
			if (tc.invalid || tc.cancelled) && m.uploads != 0 {
				t.Fatal("invalid or cancelled photo was uploaded")
			}
			if tc.outage && (!m.fail || m.invalid) {
				t.Fatal("transport failure must release for retry")
			}
		})
	}
}
