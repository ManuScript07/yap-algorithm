package application

import (
	"context"
	"errors"
	"time"
)

const MaxPhotoBytes = 5 * 1024 * 1024

var ErrInvalidPhoto = errors.New("invalid photo")

type PhotoJob struct {
	ID            string `json:"id"`
	Lease         string `json:"lease"`
	SourcePath    string `json:"source_path"`
	ImagePath     string `json:"image_path"`
	ThumbnailPath string `json:"thumbnail_path"`
	DeclaredBytes int    `json:"declared_bytes"`
	DeclaredMIME  string `json:"declared_mime"`
}

type PreparedPhoto struct {
	Image, Thumbnail []byte
	Width, Height    int
	SHA256           string
}

type MediaCleanupJob struct {
	Bucket string `json:"bucket"`
	Path   string `json:"path"`
	Lease  string `json:"lease"`
}

type MediaRepository interface {
	ClaimPhoto(context.Context) (PhotoJob, bool, error)
	CompletePhoto(context.Context, PhotoJob, PreparedPhoto) (bool, error)
	FailPhoto(context.Context, PhotoJob, bool) error
	ClaimCleanup(context.Context) (MediaCleanupJob, bool, error)
	CompleteCleanup(context.Context, MediaCleanupJob, bool) error
	Maintenance(context.Context, bool) error
}

type PhotoStorage interface {
	Download(context.Context, string) ([]byte, error)
	Upload(context.Context, string, []byte) error
	Delete(context.Context, string, string) error
}

type PhotoPreparer interface {
	Prepare(context.Context, []byte, string) (PreparedPhoto, error)
}

type PhotoProcessor struct {
	Repository MediaRepository
	Storage    PhotoStorage
	Preparer   PhotoPreparer
}

func (p PhotoProcessor) Once(ctx context.Context) (bool, error) {
	job, found, err := p.Repository.ClaimPhoto(ctx)
	if err != nil || !found {
		return false, err
	}
	data, err := p.Storage.Download(ctx, job.SourcePath)
	if err == nil && (len(data) != job.DeclaredBytes || len(data) > MaxPhotoBytes) {
		err = ErrInvalidPhoto
	}
	var photo PreparedPhoto
	if err == nil {
		photo, err = p.Preparer.Prepare(ctx, data, job.DeclaredMIME)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = p.Storage.Upload(ctx, job.ImagePath, photo.Image)
	}
	if err == nil {
		err = p.Storage.Upload(ctx, job.ThumbnailPath, photo.Thumbnail)
	}
	if err == nil {
		_, err = p.Repository.CompletePhoto(ctx, job, photo)
	}
	if err != nil && ctx.Err() == nil {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		releaseErr := p.Repository.FailPhoto(releaseCtx, job, errors.Is(err, ErrInvalidPhoto))
		cancel()
		if releaseErr != nil {
			return true, errors.New("photo failed and lease release failed")
		}
		// Invalid content is a terminal result, not a retryable worker outage.
		if errors.Is(err, ErrInvalidPhoto) {
			return true, nil
		}
	}
	return true, err
}

type MediaCleaner struct {
	Repository MediaRepository
	Storage    PhotoStorage
}

func (p MediaCleaner) Once(ctx context.Context) (bool, error) {
	job, found, err := p.Repository.ClaimCleanup(ctx)
	if err != nil || !found {
		return false, err
	}
	err = p.Storage.Delete(ctx, job.Bucket, job.Path)
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	completeErr := p.Repository.CompleteCleanup(ctx, job, err == nil)
	if completeErr != nil {
		return true, completeErr
	}
	return true, err
}
