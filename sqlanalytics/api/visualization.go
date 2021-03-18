package api

import (
	"bytes"
	"encoding/json"
)

// Visualization ...
type Visualization struct {
	ID          int             `json:"id,omitempty"`
	QueryID     string          `json:"query_id,omitempty"`
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Options     json.RawMessage `json:"options,omitempty"`

	// The query this visualization belongs to is only set when retrieving a dashboard.
	Query json.RawMessage `json:"query,omitempty"`
}

// VisualizationTableColumn ...
type VisualizationTableColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Title       string `json:"title"`
	DisplayAs   string `json:"displayAs"`
	AllowSearch bool   `json:"allowSearch"`

	NumberFormat       string   `json:"numberFormat"`
	BooleanValues      []string `json:"booleanValues"`
	ImageURLTemplate   string   `json:"imageUrlTemplate"`
	ImageTitleTemplate string   `json:"imageTitleTemplate"`
	ImageWidth         string   `json:"imageWidth"`
	ImageHeight        string   `json:"imageHeight"`
	LinkURLTemplate    string   `json:"linkUrlTemplate"`
	LinkTextTemplate   string   `json:"linkTextTemplate"`
	LinkTitleTemplate  string   `json:"linkTitleTemplate"`
	LinkOpenInNewTab   bool     `json:"linkOpenInNewTab"`
	Visible            bool     `json:"visible"`
	Order              int      `json:"order,omitempty"`
	AlignContent       string   `json:"alignContent"`
	AllowHTML          bool     `json:"allowHTML"`
	HighlightLinks     bool     `json:"highlightLinks"`

	// Set this to skip fields with default values when marshalling.
	SkipDefaults bool `json:"-"`
}

var visualizationTableColumnDefaults = VisualizationTableColumn{
	Name:        "",
	Type:        "",
	Title:       "",
	DisplayAs:   "",
	AllowSearch: false,

	NumberFormat:       "",
	BooleanValues:      []string{"false", "true"},
	ImageURLTemplate:   "{{ @ }}",
	ImageTitleTemplate: "{{ @ }}",
	LinkURLTemplate:    "{{ @ }}",
	LinkTextTemplate:   "{{ @ }}",
	LinkTitleTemplate:  "{{ @ }}",
	LinkOpenInNewTab:   true,
	Visible:            true,
	Order:              0,
	AlignContent:       "left",
	AllowHTML:          true,
	HighlightLinks:     false,
}

// MarshalJSON ...
func (v VisualizationTableColumn) MarshalJSON() ([]byte, error) {
	type localVisualizationTableColumn VisualizationTableColumn
	thisBytes, err := json.Marshal((localVisualizationTableColumn)(v))
	if err != nil {
		return nil, err
	}
	if !v.SkipDefaults {
		return thisBytes, nil
	}

	defaultsBytes, err := json.Marshal((localVisualizationTableColumn)(visualizationTableColumnDefaults))
	if err != nil {
		return nil, err
	}

	// Unmarshal into map, so we can do key-by-key comparison.
	this := make(map[string]json.RawMessage)
	if err = json.Unmarshal(thisBytes, &this); err != nil {
		return nil, err
	}
	defaults := make(map[string]json.RawMessage)
	if err = json.Unmarshal(defaultsBytes, &defaults); err != nil {
		return nil, err
	}

	for k, v := range defaults {
		vt, ok := this[k]
		if !ok {
			continue
		}

		// If the value if equal to the default value, remove it from the output map.
		if bytes.Equal(vt, v) {
			delete(this, k)
		}
	}

	return json.Marshal(this)
}

// VisualizationTableOptions ...
type VisualizationTableOptions struct {
	ItemsPerPage int                        `json:"itemsPerPage,omitempty"`
	Columns      []VisualizationTableColumn `json:"columns,omitempty"`
}
