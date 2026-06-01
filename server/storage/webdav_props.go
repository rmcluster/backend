package storage

import (
	"bytes"
	"context"
	"encoding/xml"
	"sort"

	"github.com/rmcluster/backend/server/gcas"
	xwebdav "golang.org/x/net/webdav"
)

const webdavPropNamespace = "urn:rmcluster:webdav"

var webdavDevicesPropName = xml.Name{Space: webdavPropNamespace, Local: "devices"}

type gcasDeviceProvider interface {
	DevicesForHashes(ctx context.Context, hashes []gcas.Hash) ([]gcas.DeviceDisplay, error)
}

func devicePropertyForHashes(ctx context.Context, cas gcas.GCAS, hashes []gcas.Hash) (xwebdav.Property, bool, error) {
	provider, ok := cas.(gcasDeviceProvider)
	if !ok {
		return xwebdav.Property{}, false, nil
	}

	devices, err := provider.DevicesForHashes(ctx, hashes)
	if err != nil {
		return xwebdav.Property{}, false, err
	}
	if len(devices) == 0 {
		return xwebdav.Property{}, false, nil
	}

	sort.Slice(devices, func(i, j int) bool {
		return devices[i].DisplayName < devices[j].DisplayName
	})

	var inner bytes.Buffer
	enc := xml.NewEncoder(&inner)
	for _, device := range devices {
		start := xml.StartElement{
			Name: xml.Name{Space: webdavPropNamespace, Local: "device"},
			Attr: []xml.Attr{
				{Name: xml.Name{Local: "xmlns:rmc"}, Value: webdavPropNamespace},
			},
		}
		if err := enc.EncodeToken(start); err != nil {
			return xwebdav.Property{}, false, err
		}
		if err := enc.EncodeToken(xml.CharData([]byte(device.DisplayName))); err != nil {
			return xwebdav.Property{}, false, err
		}
		if err := enc.EncodeToken(start.End()); err != nil {
			return xwebdav.Property{}, false, err
		}
	}
	if err := enc.Flush(); err != nil {
		return xwebdav.Property{}, false, err
	}

	return xwebdav.Property{
		XMLName:  webdavDevicesPropName,
		InnerXML: inner.Bytes(),
	}, true, nil
}
