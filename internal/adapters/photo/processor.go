// Package photo prepares untrusted uploads without retaining EXIF/GPS, PNG
// ancillary chunks or trailing data. Bounds are checked before full decoding.
package photo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"image"
	"image/color"
	"image/jpeg"
	_ "image/png"

	"golang.org/x/image/draw"
	"yap-algorithm/internal/application"
)

const MaxPixels = 20_000_000
const MaxDimension = 8192

type Processor struct{}

func (Processor) Prepare(ctx context.Context, data []byte, mime string) (application.PreparedPhoto, error) {
	var out application.PreparedPhoto
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if len(data) == 0 || len(data) > application.MaxPhotoBytes {
		return out, application.ErrInvalidPhoto
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || !((format == "jpeg" && mime == "image/jpeg") || (format == "png" && mime == "image/png")) ||
		config.Width < 1 || config.Height < 1 || config.Width > MaxDimension || config.Height > MaxDimension ||
		int64(config.Width)*int64(config.Height) > MaxPixels {
		return out, application.ErrInvalidPhoto
	}
	source, _, err := image.Decode(bytes.NewReader(data))
	if err != nil || source.Bounds().Dx() != config.Width || source.Bounds().Dy() != config.Height {
		return out, application.ErrInvalidPhoto
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	var view image.Image = source
	if orientation := orientation(data, format); orientation != 1 {
		view = oriented{Image: source, orientation: orientation}
	}
	full := resize(view, 2048)
	if err := ctx.Err(); err != nil {
		return out, err
	}
	thumb := resize(full, 512)
	var encoded, preview bytes.Buffer
	if err := jpeg.Encode(&encoded, full, &jpeg.Options{Quality: 85}); err != nil {
		return out, err
	}
	if err := jpeg.Encode(&preview, thumb, &jpeg.Options{Quality: 80}); err != nil {
		return out, err
	}
	if encoded.Len() > application.MaxPhotoBytes || preview.Len() > application.MaxPhotoBytes {
		return out, application.ErrInvalidPhoto
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	digest := sha256.Sum256(encoded.Bytes())
	return application.PreparedPhoto{Image: encoded.Bytes(), Thumbnail: preview.Bytes(), Width: full.Bounds().Dx(),
		Height: full.Bounds().Dy(), SHA256: hex.EncodeToString(digest[:])}, nil
}

func resize(source image.Image, maxEdge int) *image.RGBA {
	w, h := source.Bounds().Dx(), source.Bounds().Dy()
	if w > maxEdge || h > maxEdge {
		if w >= h {
			h = max(1, h*maxEdge/w)
			w = maxEdge
		} else {
			w = max(1, w*maxEdge/h)
			h = maxEdge
		}
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.Draw(dst, dst.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	draw.CatmullRom.Scale(dst, dst.Bounds(), source, source.Bounds(), draw.Over, nil)
	return dst
}

// An image view applies EXIF orientation while scaling, avoiding another full
// size pixel allocation. Only the orientation value survives metadata parsing.
type oriented struct {
	image.Image
	orientation int
}

func (o oriented) Bounds() image.Rectangle {
	w, h := o.Image.Bounds().Dx(), o.Image.Bounds().Dy()
	if o.orientation >= 5 {
		w, h = h, w
	}
	return image.Rect(0, 0, w, h)
}
func (o oriented) At(x, y int) color.Color {
	x, y = o.sourcePoint(x, y)
	return o.Image.At(x, y)
}

func (o oriented) RGBA64At(x, y int) color.RGBA64 {
	x, y = o.sourcePoint(x, y)
	if source, ok := o.Image.(image.RGBA64Image); ok {
		return source.RGBA64At(x, y)
	}
	r, g, b, a := o.Image.At(x, y).RGBA()
	return color.RGBA64{uint16(r), uint16(g), uint16(b), uint16(a)}
}

func (o oriented) sourcePoint(x, y int) (int, int) {
	b := o.Image.Bounds()
	w, h := b.Dx(), b.Dy()
	switch o.orientation {
	case 2:
		x = w - 1 - x
	case 3:
		x, y = w-1-x, h-1-y
	case 4:
		y = h - 1 - y
	case 5:
		x, y = y, x
	case 6:
		x, y = y, h-1-x
	case 7:
		x, y = w-1-y, h-1-x
	case 8:
		x, y = w-1-y, x
	}
	return b.Min.X + x, b.Min.Y + y
}

func orientation(data []byte, format string) int {
	if format == "jpeg" {
		for offset := 2; offset+4 <= len(data); {
			if data[offset] != 0xff || data[offset+1] == 0xda || data[offset+1] == 0xd9 {
				break
			}
			length := int(binary.BigEndian.Uint16(data[offset+2:]))
			if length < 2 || offset+2+length > len(data) {
				break
			}
			segment := data[offset+4 : offset+2+length]
			if data[offset+1] == 0xe1 && bytes.HasPrefix(segment, []byte("Exif\x00\x00")) {
				return tiffOrientation(segment[6:])
			}
			offset += 2 + length
		}
	} else if format == "png" {
		for offset := 8; offset+12 <= len(data); {
			length := int64(binary.BigEndian.Uint32(data[offset:]))
			if length > int64(len(data)-offset-12) {
				break
			}
			if string(data[offset+4:offset+8]) == "eXIf" {
				return tiffOrientation(data[offset+8 : offset+8+int(length)])
			}
			offset += 12 + int(length)
		}
	}
	return 1
}

func tiffOrientation(data []byte) int {
	if len(data) < 8 {
		return 1
	}
	var order binary.ByteOrder
	switch string(data[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 1
	}
	if order.Uint16(data[2:4]) != 42 {
		return 1
	}
	offset := int64(order.Uint32(data[4:8]))
	if offset < 8 || offset+2 > int64(len(data)) {
		return 1
	}
	count := min(128, int(order.Uint16(data[offset:offset+2])))
	for i := 0; i < count; i++ {
		start := int(offset) + 2 + 12*i
		if start+12 > len(data) {
			break
		}
		entry := data[start : start+12]
		if order.Uint16(entry[:2]) == 0x112 && order.Uint16(entry[2:4]) == 3 && order.Uint32(entry[4:8]) == 1 {
			value := int(order.Uint16(entry[8:10]))
			if value >= 1 && value <= 8 {
				return value
			}
		}
	}
	return 1
}
