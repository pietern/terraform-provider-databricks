package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/databrickslabs/terraform-provider-databricks/sqlanalytics/api"
)

func canonicalize(str string) string {
	str = regexp.MustCompile(`[()]`).ReplaceAllLiteralString(str, "")
	str = regexp.MustCompile(`\W`).ReplaceAllLiteralString(str, "_")
	return strings.ToLower(str)
}

func loadQuery(path string) (*api.Query, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var q api.Query
	dec := json.NewDecoder(f)
	err = dec.Decode(&q)
	if err != nil {
		return nil, err
	}

	return &q, nil
}

func loadDashboard(path string) (*api.Dashboard, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var d api.Dashboard
	dec := json.NewDecoder(f)
	err = dec.Decode(&d)
	if err != nil {
		return nil, err
	}

	return &d, nil
}

func processQuery(path string) {
	q, err := loadQuery(path)
	if err != nil {
		panic(err)
	}

	queryResourceName := canonicalize(q.Name)
	o, err := os.Create(fmt.Sprintf("query_%s.tf", queryResourceName))
	if err != nil {
		panic(err)
	}
	defer o.Close()

	x := func(format string, a ...interface{}) {
		_, err := fmt.Fprintf(o, format+"\n", a...)
		if err != nil {
			panic(err)
		}
	}

	xRaw := func(str string) {
		_, err := fmt.Fprintln(o, str)
		if err != nil {
			panic(err)
		}
	}

	xStrings := func(field string, vs []string) {
		x(`%s = [`, field)
		for _, v := range vs {
			x("%s,", strconv.Quote(v))
		}
		x(`]`)
	}

	x(`resource "databricks_sql_query" "%s" {`, queryResourceName)
	x("data_source_id = %s", strconv.Quote(q.DataSourceID))
	x(`name = %s`, strconv.Quote(q.Name))

	if q.Description != "" {
		x(`description = %s`, strconv.Quote(q.Description))
	}

	x(``)
	xStrings(`tags`, q.Tags)

	if q.Schedule != nil {
		x(``)
		x(`schedule {`)
		x(`interval = %d`, q.Schedule.Interval)
		x(`}`)
	}

	for _, p := range q.Options.Parameters {
		wrap := func(qp api.QueryParameter, f func()) {
			x(``)
			x(`parameter {`)
			x(`name = %s`, strconv.Quote(qp.Name))
			if qp.Title != "" {
				x(`title = %s`, strconv.Quote(qp.Title))
			}
			x(``)
			f()
			x(`}`)
		}

		switch qp := p.(type) {
		case *api.QueryParameterText:
			wrap(qp.QueryParameter, func() {
				x(`text {`)
				x(`value = %s`, strconv.Quote(qp.Value))
				x(`}`)
			})
		case *api.QueryParameterNumber:
			wrap(qp.QueryParameter, func() {
				x(`number {`)
				x(`value = %d`, int(qp.Value))
				x(`}`)
			})
		case *api.QueryParameterEnum:
			wrap(qp.QueryParameter, func() {
				x(`enum {`)
				xStrings(`options`, strings.Split(qp.Options, "\n"))
				if qp.Multi != nil {
					xStrings(`values`, qp.Values)
					x(``)
					x(`multiple {`)
					x(`prefix = %s`, strconv.Quote(qp.Multi.Prefix))
					x(`suffix = %s`, strconv.Quote(qp.Multi.Suffix))
					x(`separator = %s`, strconv.Quote(qp.Multi.Separator))
					x(`}`)
				} else {
					x(`value = %s`, strconv.Quote(qp.Values[0]))
				}
				x(`}`)
			})
		case *api.QueryParameterQuery:
			wrap(qp.QueryParameter, func() {
				x(`query {`)
				x(`query_id = %s`, strconv.Quote(qp.QueryID))
				if qp.Multi != nil {
					xStrings(`values`, qp.Values)
					x(``)
					x(`multiple {`)
					x(`prefix = %s`, strconv.Quote(qp.Multi.Prefix))
					x(`suffix = %s`, strconv.Quote(qp.Multi.Suffix))
					x(`separator = %s`, strconv.Quote(qp.Multi.Separator))
					x(`}`)
				} else {
					x(`value = %s`, strconv.Quote(qp.Values[0]))
				}
				x(`}`)
			})
		case *api.QueryParameterDate:
			wrap(qp.QueryParameter, func() {
				x(`date {`)
				x(`value = %s`, strconv.Quote(qp.Value))
				x(`}`)
			})
		case *api.QueryParameterDateTime:
			wrap(qp.QueryParameter, func() {
				x(`datetime {`)
				x(`value = %s`, strconv.Quote(qp.Value))
				x(`}`)
			})
		case *api.QueryParameterDateTimeSec:
			wrap(qp.QueryParameter, func() {
				x(`datetimesec {`)
				x(`value = %s`, strconv.Quote(qp.Value))
				x(`}`)
			})
		case *api.QueryParameterDateRange:
			wrap(qp.QueryParameter, func() {
				x(`date_range {`)
				x(`value = %s`, strconv.Quote(qp.Value))
				x(`}`)
			})
		case *api.QueryParameterDateTimeRange:
			wrap(qp.QueryParameter, func() {
				x(`datetime_range {`)
				x(`value = %s`, strconv.Quote(qp.Value))
				x(`}`)
			})
		case *api.QueryParameterDateTimeSecRange:
			wrap(qp.QueryParameter, func() {
				x(`datetimesec_range {`)
				x(`value = %s`, strconv.Quote(qp.Value))
				x(`}`)
			})
		default:
			log.Fatalf("Don't know what to do for type: %#v", reflect.TypeOf(p).String())
		}
	}

	x(``)
	x("query = <<SQL")
	xRaw(q.Query)
	x("SQL")

	x(`}`)

	// Move on to visualizations.
	visualizationTypeCounter := make(map[string]int)
	for _, viz := range q.Visualizations {
		var v api.Visualization
		err = json.Unmarshal(viz, &v)
		if err != nil {
			panic(err)
		}

		typ := strings.ToLower(v.Type)
		seq := visualizationTypeCounter[typ]
		visualizationTypeCounter[typ]++
		visualizationResourceName := fmt.Sprintf("%s_%s_%d", queryResourceName, typ, seq)

		// Sanitize options to remove superfluous defaults.
		if typ == "table" {
			var to api.VisualizationTableOptions
			if err = json.Unmarshal(v.Options, &to); err != nil {
				panic(err)
			}
			// Ignore default values when re-marshalling.
			for i := range to.Columns {
				to.Columns[i].SkipDefaults = true
				// Remove order field; order is implied from array order.
				to.Columns[i].Order = 0
			}
			// Re-marshal table options without default values.
			if v.Options, err = json.MarshalIndent(to, "", "  "); err != nil {
				panic(err)
			}
		} else {
			var iface interface{}
			if err = json.Unmarshal(v.Options, &iface); err != nil {
				panic(err)
			}
			if v.Options, err = json.MarshalIndent(iface, "", "  "); err != nil {
				panic(err)
			}
		}

		x(``)
		x(`resource "databricks_sql_visualization" "%s" {`, visualizationResourceName)
		x(`query_id = databricks_sql_query.%s.id`, queryResourceName)
		x(`type = %s`, strconv.Quote(typ))
		x(`name = %s`, strconv.Quote(v.Name))
		if v.Description != "" {
			x("description = %s", strconv.Quote(v.Description))
		}
		x(``)
		x(`options = <<JSON`)
		xRaw(string(v.Options))
		x(`JSON`)
		x(`}`)
	}
}

func processDashboard(path string) {
	d, err := loadDashboard(path)
	if err != nil {
		panic(err)
	}

	dashboardResourceName := canonicalize(d.Name)
	o, err := os.Create(fmt.Sprintf("dashboard_%s.tf", dashboardResourceName))
	if err != nil {
		panic(err)
	}
	defer o.Close()

	x := func(format string, a ...interface{}) {
		_, err := fmt.Fprintf(o, format+"\n", a...)
		if err != nil {
			panic(err)
		}
	}

	xRaw := func(str string) {
		_, err := fmt.Fprintln(o, str)
		if err != nil {
			panic(err)
		}
	}

	xStrings := func(field string, vs []string) {
		x(`%s = [`, field)
		for _, v := range vs {
			x("%s,", strconv.Quote(v))
		}
		x(`]`)
	}

	x(`resource "databricks_sql_dashboard" "%s" {`, dashboardResourceName)
	x(`name = %s`, strconv.Quote(d.Name))
	x(``)
	xStrings(`tags`, d.Tags)
	x(`}`)

	// Move on to widgets.
	counter := 0
	for _, widget := range d.Widgets {
		var w api.Widget
		err = json.Unmarshal(widget, &w)
		if err != nil {
			panic(err)
		}

		seq := counter
		counter++
		widgetResourceName := fmt.Sprintf("%s_%d", dashboardResourceName, seq)

		x(``)
		x(`resource "databricks_sql_widget" "%s" {`, widgetResourceName)
		x(`dashboard_id = databricks_sql_dashboard.%s.id`, dashboardResourceName)
		if w.Visualization != nil {
			var v api.Visualization
			err = json.Unmarshal(w.Visualization, &v)
			if err != nil {
				panic(err)
			}
			x(`visualization_id = "%d"`, v.ID)
		} else {
			x(`text = <<EOT`)
			if w.Text != nil {
				xRaw(*w.Text)
			}
			x(`EOT`)
		}

		if p := w.Options.Position; p != nil {
			x(``)
			x(`position {`)
			x(`size_x = %d`, p.SizeX)
			x(`size_y = %d`, p.SizeY)
			x(`pos_x = %d`, p.PosX)
			x(`pos_y = %d`, p.PosY)
			x(`}`)
		}

		for _, pv := range w.Options.ParameterMapping {
			x(``)
			x(`parameter {`)
			x(`name = %s`, strconv.Quote(pv.Name))
			x(`type = %s`, strconv.Quote(pv.Type))
			if pv.MapTo != "" {
				x(`map_to = %s`, strconv.Quote(pv.MapTo))
			}
			if pv.Title != "" {
				x(`title = %s`, strconv.Quote(pv.Title))
			}
			switch pvt := pv.Value.(type) {
			case nil:
				// Nothing
			case string:
				x(`value = %s`, strconv.Quote(pvt))
			default:
				panic(fmt.Errorf("Unhandled value type: %#v", reflect.TypeOf(pv.Value)))
			}
			x(`}`)
		}

		x(`}`)
	}
}

func main() {
	flag.Parse()
	for _, arg := range flag.Args() {
		processDashboard(arg)
	}
}
