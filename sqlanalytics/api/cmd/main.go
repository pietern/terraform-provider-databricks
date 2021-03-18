package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/databrickslabs/terraform-provider-databricks/common"
	"github.com/databrickslabs/terraform-provider-databricks/sqlanalytics/api"
)

type Dashboard struct {
	RemoteID     string
	ResourceName string

	Object *api.Dashboard
}

type Widget struct {
	RemoteID     string
	ResourceName string

	Object *api.Widget
}

type Query struct {
	RemoteID     string
	ResourceName string

	Object *api.Query
}

type Visualization struct {
	RemoteID     string
	ResourceName string

	Object *api.Visualization
}

type Inventory struct {
	sqla *api.Wrapper

	Dashboards     []Dashboard
	Widgets        []Widget
	Queries        []Query
	Visualizations []Visualization
}

func (i *Inventory) loadDashboard(id string) {
	for _, dp := range i.Dashboards {
		if dp.RemoteID == id {
			return
		}
	}

	d, err := i.sqla.ReadDashboard(&api.Dashboard{ID: id})
	if err != nil {
		panic(err)
	}

	dp := Dashboard{
		RemoteID:     d.ID,
		ResourceName: canonicalize(d.Name),
		Object:       d,
	}

	wps := []Widget{}
	for i, widget := range d.Widgets {
		var w api.Widget
		err := json.Unmarshal(widget, &w)
		if err != nil {
			panic(err)
		}

		// Explicitly link widget to dashboard.
		w.DashboardID = d.ID

		// Load visualization ID if present.
		if len(w.Visualization) != 0 {
			var v api.Visualization
			err := json.Unmarshal(w.Visualization, &v)
			if err != nil {
				panic(err)
			}

			w.VisualizationID = &v.ID
		}

		wp := Widget{
			RemoteID:     strconv.Itoa(w.ID),
			ResourceName: fmt.Sprintf("%s_%d", dp.ResourceName, i),
			Object:       &w,
		}

		wps = append(wps, wp)
	}

	i.Dashboards = append(i.Dashboards, dp)
	i.Widgets = append(i.Widgets, wps...)

	// Now recurse into widgets to find visualizations.
	for _, wp := range wps {
		if len(wp.Object.Visualization) == 0 {
			continue
		}

		var v api.Visualization
		err := json.Unmarshal(wp.Object.Visualization, &v)
		if err != nil {
			panic(err)
		}

		if v.Query != nil {
			var q api.Query
			err := json.Unmarshal(v.Query, &q)
			if err != nil {
				panic(err)
			}

			i.loadQuery(q.ID)
		}
	}
}

func (i *Inventory) loadQuery(id string) {
	for _, qp := range i.Queries {
		if qp.RemoteID == id {
			return
		}
	}

	q, err := i.sqla.ReadQuery(&api.Query{ID: id})
	if err != nil {
		panic(err)
	}

	qp := Query{
		RemoteID:     q.ID,
		ResourceName: canonicalize(q.Name),
		Object:       q,
	}

	vps := []Visualization{}
	for _, visualization := range qp.Object.Visualizations {
		var v api.Visualization
		err := json.Unmarshal(visualization, &v)
		if err != nil {
			panic(err)
		}

		// Explicitly link visualization to query.
		v.QueryID = q.ID

		vp := Visualization{
			RemoteID:     strconv.Itoa(v.ID),
			ResourceName: "",
			Object:       &v,
		}

		vps = append(vps, vp)
	}

	// Figure out number of visualizations per type.
	// If there's only one, we don't need to suffix the index.
	vtyp := make(map[string]int)
	for _, vp := range vps {
		typ := strings.ToLower(vp.Object.Type)
		vtyp[typ]++
	}

	// Second pass to synthesize resource name for visualization.
	vtypSeq := make(map[string]int)
	for i := range vps {
		vp := &vps[i]
		typ := strings.ToLower(vp.Object.Type)
		if vtyp[typ] == 1 {
			vp.ResourceName = fmt.Sprintf("%s_%s", qp.ResourceName, typ)
		} else {
			vp.ResourceName = fmt.Sprintf("%s_%s_%d", qp.ResourceName, typ, vtypSeq[typ])
			vtypSeq[typ]++
		}
	}

	i.Queries = append(i.Queries, qp)
	i.Visualizations = append(i.Visualizations, vps...)
}

func (i *Inventory) writeQueries() {
	for _, qp := range i.Queries {
		i.writeQuery(qp)
	}
}

func (i *Inventory) writeQuery(qp Query) {
	o, err := os.Create(fmt.Sprintf("query_%s.tf", qp.ResourceName))
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

	q := qp.Object

	x(`resource "databricks_sql_query" "%s" {`, qp.ResourceName)
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
	for _, vp := range i.Visualizations {
		if vp.Object.QueryID != qp.RemoteID {
			continue
		}

		v := vp.Object
		typ := strings.ToLower(v.Type)

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
		x(`resource "databricks_sql_visualization" "%s" {`, vp.ResourceName)
		x(`query_id = databricks_sql_query.%s.id`, qp.ResourceName)
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

func (i *Inventory) writeDashboards() {
	for _, dp := range i.Dashboards {
		i.writeDashboard(dp)
	}
}

func (i *Inventory) writeDashboard(dp Dashboard) {
	o, err := os.Create(fmt.Sprintf("dashboard_%s.tf", dp.ResourceName))
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

	d := dp.Object

	x(`resource "databricks_sql_dashboard" "%s" {`, dp.ResourceName)
	x(`name = %s`, strconv.Quote(d.Name))
	x(``)
	xStrings(`tags`, d.Tags)
	x(`}`)

	// Move on to widgets.
	for _, wp := range i.Widgets {
		if wp.Object.DashboardID != dp.RemoteID {
			continue
		}

		w := wp.Object

		x(``)
		x(`resource "databricks_sql_widget" "%s" {`, wp.ResourceName)
		x(`dashboard_id = databricks_sql_dashboard.%s.id`, dp.ResourceName)
		if w.VisualizationID != nil {
			// Look up the right visualization.
			var vp *Visualization
			for _, vpp := range i.Visualizations {
				if vpp.RemoteID == strconv.Itoa(*w.VisualizationID) {
					vp = &vpp
					break
				}
			}

			if vp == nil {
				log.Fatalf("Couldn't find visualization...")
			}

			x(`visualization_id = databricks_sql_visualization.%s.id`, vp.ResourceName)
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

func canonicalize(str string) string {
	str = regexp.MustCompile(`[()]`).ReplaceAllLiteralString(str, "")
	str = regexp.MustCompile(`\W`).ReplaceAllLiteralString(str, "_")
	return strings.ToLower(str)
}

var inventory Inventory

var profile = flag.String("profile", "", "Profile name in ~/.databrickscfg to use.")
var mode = flag.String("mode", "dashboard", `Pick "dashboard" or "query" mode.`)

func main() {
	flag.Parse()

	client := common.DatabricksClient{Profile: *profile}
	if err := client.Configure(); err != nil {
		panic(err)
	}

	sqla := api.NewWrapper(context.Background(), &client)
	inv := Inventory{sqla: &sqla}

	switch *mode {
	case "dashboard":
		for _, arg := range flag.Args() {
			inv.loadDashboard(arg)
		}
	case "query":
		for _, arg := range flag.Args() {
			inv.loadQuery(arg)
		}
	default:
		log.Fatalf(`Unknown mode: %s`, *mode)
	}

	inv.writeQueries()
	inv.writeDashboards()
}
