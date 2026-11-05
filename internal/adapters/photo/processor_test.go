package photo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"

	"yap-algorithm/internal/application"
)

func sample(t testing.TB, width, height int) []byte {
	t.Helper()
	im := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			im.SetRGBA(x, y, color.RGBA{uint8(x), uint8(y), uint8(x + y), 255})
		}
	}
	var out bytes.Buffer
	if err := jpeg.Encode(&out, im, &jpeg.Options{Quality: 85}); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestPrepareRemovesMetadataAndKeepsOrientation(t *testing.T) {
	data := sample(t, 12, 6)
	exif := []byte("Exif\x00\x00II\x2a\x00\x08\x00\x00\x00\x01\x00\x12\x01\x03\x00\x01\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00GPS-PRIVATE-METADATA")
	segment := []byte{255, 225, 0, 0}
	binary.BigEndian.PutUint16(segment[2:], uint16(len(exif)+2))
	input := append(append(append(append([]byte{}, data[:2]...), segment...), exif...), data[2:]...)
	input = append(input, []byte("TRAILING-PRIVATE-DATA")...)
	photo, err := (Processor{}).Prepare(context.Background(), input, "image/jpeg")
	if err != nil || photo.Width != 6 || photo.Height != 12 || len(photo.SHA256) != 64 {
		t.Fatalf("orientation/shape: %+v, %v", photo, err)
	}
	for _, output := range [][]byte{photo.Image, photo.Thumbnail} {
		if bytes.Contains(output, []byte("Exif")) || bytes.Contains(output, []byte("PRIVATE")) {
			t.Fatal("private metadata survived re-encoding")
		}
		if _, err := jpeg.Decode(bytes.NewReader(output)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBoundsTypesTransparencyAndCancellation(t *testing.T) {
	valid := sample(t, 40, 20)
	for _, tc := range []struct {
		name, mime string
		data       []byte
	}{
		{"mismatched MIME", "image/png", valid},
		{"truncated", "image/jpeg", valid[:len(valid)/2]},
		{"oversized", "image/jpeg", make([]byte, application.MaxPhotoBytes+1)},
		{"script", "image/jpeg", []byte("<script>alert(1)</script>")},
		{"empty", "image/png", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := (Processor{}).Prepare(context.Background(), tc.data, tc.mime); !errors.Is(err, application.ErrInvalidPhoto) {
				t.Fatalf("invalid upload accepted: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Processor{}).Prepare(ctx, valid, "image/jpeg"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled work was decoded")
	}
	var transparent bytes.Buffer
	png.Encode(&transparent, image.NewNRGBA(image.Rect(0, 0, 2, 2)))
	out, err := (Processor{}).Prepare(context.Background(), transparent.Bytes(), "image/png")
	if err != nil {
		t.Fatal(err)
	}
	decoded, _ := jpeg.Decode(bytes.NewReader(out.Image))
	r, g, b, _ := decoded.At(0, 0).RGBA()
	if r < 65000 || g < 65000 || b < 65000 {
		t.Fatal("transparency was not composited on white")
	}
	// A valid PNG header with huge dimensions must be rejected before any full
	// image allocation, even when no pixel stream follows it.
	var normal bytes.Buffer
	png.Encode(&normal, image.NewRGBA(image.Rect(0, 0, 1, 1)))
	header := append([]byte{}, normal.Bytes()...)
	binary.BigEndian.PutUint32(header[16:20], 100000)
	binary.BigEndian.PutUint32(header[20:24], 100000)
	binary.BigEndian.PutUint32(header[29:33], crc32.ChecksumIEEE(header[12:29]))
	if config, _, err := image.DecodeConfig(bytes.NewReader(header)); err != nil || config.Width != 100000 {
		t.Fatal("invalid decompression-bound test fixture")
	}
	if _, err := (Processor{}).Prepare(context.Background(), header, "image/png"); !errors.Is(err, application.ErrInvalidPhoto) {
		t.Fatal("decompression bound bypassed")
	}
}

func TestAllOrientations(t *testing.T) {
	source := image.NewGray(image.Rect(0, 0, 2, 3))
	for i := range source.Pix {
		source.Pix[i] = uint8(i + 1)
	}
	want := [][]byte{{1, 2, 3, 4, 5, 6}, {2, 1, 4, 3, 6, 5}, {6, 5, 4, 3, 2, 1}, {5, 6, 3, 4, 1, 2},
		{1, 3, 5, 2, 4, 6}, {5, 3, 1, 6, 4, 2}, {6, 4, 2, 5, 3, 1}, {2, 4, 6, 1, 3, 5}}
	for orientation := 1; orientation <= 8; orientation++ {
		view := oriented{source, orientation}
		var pixels []byte
		for y := 0; y < view.Bounds().Dy(); y++ {
			for x := 0; x < view.Bounds().Dx(); x++ {
				pixels = append(pixels, color.GrayModel.Convert(view.At(x, y)).(color.Gray).Y)
			}
		}
		if !bytes.Equal(pixels, want[orientation-1]) {
			t.Fatalf("orientation %d: %v", orientation, pixels)
		}
	}
}

func FuzzOrientation(f *testing.F) {
	f.Add([]byte("Exif\x00\x00II*\x00"))
	f.Add([]byte{255, 216, 255, 225, 0, 2, 255, 217})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65536 {
			return
		}
		for _, value := range []int{tiffOrientation(data), orientation(data, "jpeg"), orientation(data, "png")} {
			if value < 1 || value > 8 {
				t.Fatal("invalid orientation")
			}
		}
	})
}

func BenchmarkPrepare12MP(b *testing.B) {
	data := sample(b, 4000, 3000)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := (Processor{}).Prepare(context.Background(), data, "image/jpeg"); err != nil {
			b.Fatal(err)
		}
	}
}
