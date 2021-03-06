package main

import (
	"fmt"
	"log"

	"encoding/json"
	"os"
	"sync"

	"bytes"

	"github.com/valyala/fasthttp"
)

type User struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	BirthDate int    `json:"birth_date"`
	Gender    string `json:"gender"`
	ID        int    `json:"id"`
	Email     string `json:"email"`
}

type Location struct {
	Distance int    `json:"distance"`
	City     string `json:"city"`
	Place    string `json:"place"`
	ID       int    `json:"id"`
	Country  string `json:"country"`
}

type Visit struct {
	User      int `json:"user"`
	Location  int `json:"location"`
	VisitedAt int `json:"visited_at"`
	ID        int `json:"id"`
	Mark      int `json:"mark"`
}

type JSUsers struct {
	Users []User `json:"users"`
}
type JSVisits struct {
	Visits []Visit `json:"visits"`
}

type JSLocations struct {
	Locations []Location `json:"locations"`
}

var (
	VisMap  map[int]*Visit    = make(map[int]*Visit)
	LocMap  map[int]*Location = make(map[int]*Location)
	UserMap map[int]*User     = make(map[int]*User)

	JSONUsers     JSUsers     = JSUsers{}
	JSONVisits    JSVisits    = JSVisits{}
	JSONLocations JSLocations = JSLocations{}
)

func FillUsMap() {
	for k, v := range JSONUsers.Users {
		UserMap[v.ID] = &JSONUsers.Users[k]
	}
}
func FillLocMap() {
	for k, v := range JSONLocations.Locations {
		LocMap[v.ID] = &JSONLocations.Locations[k]
	}
}

func FillVisMap() {
	for k, v := range JSONVisits.Visits {
		VisMap[v.ID] = &JSONVisits.Visits[k]
	}
}

var path = "/Users/aleksandr/hlcupdocs/data/TRAIN/data/"

func init() {

	wg := sync.WaitGroup{}
	wg.Add(3)
	for _, r := range []struct {
		file string
		data interface{}
		fill func()
	}{
		{path + "visits_1.json", &JSONVisits, FillVisMap},
		{path + "locations_1.json", &JSONLocations, FillLocMap},
		{path + "users_1.json", &JSONUsers, FillUsMap},
	} {
		go func(filename string, d interface{}, fill func()) {
			defer wg.Done()
			f, err := os.OpenFile(filename, os.O_RDONLY, 0)
			if err != nil {
				panic(err)
			}
			defer f.Close()

			decoder := json.NewDecoder(f)
			if err := decoder.Decode(d); err != nil {
				panic(err)
			}

			fill()
		}(r.file, r.data, r.fill)
	}
	wg.Wait()

}

// /<entity>/<id>
var EntityUsers = []byte("users")
var EntityVisits = []byte("visits")
var EntityLocations = []byte("locations")

func fastHTTPHandler(ctx *fasthttp.RequestCtx) {

	p := bytes.Split(ctx.RequestURI(), []byte{'/'})

	switch {
	case bytes.Equal(p[1], EntityUsers), bytes.Equal(p[1], EntityLocations), bytes.Equal(p[1], EntityVisits):

		ctx.SetStatusCode(fasthttp.StatusNotFound)

		var id int = -1
		if len(p[2]) > 0 {
			var base = 1
			for i := len(p[2]) - 1; i >= 0; i-- {
				if !('0' <= p[2][i] && p[2][i] <= '9') {
					id = -1
					break
				}

				id += int(p[2][i]-'0') * base
				base *= 10
			}

			//fmt.Printf("ID: %d ", id)

			//id = cast.ToInt(string(p[2])) // fucking very slow
		}

		var entity interface{}
		var ok bool
		if bytes.Equal(p[1], EntityUsers) {
			entity, ok = UserMap[id]
		} else if bytes.Equal(p[1], EntityLocations) {
			entity, ok = LocMap[id]
		} else {
			entity, ok = VisMap[id]
		}

		if ok {
			buf, err := json.Marshal(entity)
			if err != nil {
				panic(err)
			}
			ctx.SetStatusCode(fasthttp.StatusOK)
			fmt.Fprintf(ctx, "%s", buf)
		}

	default:
		fmt.Fprintf(ctx, "Hi there! RequestURI is %q", ctx.RequestURI())
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	}

}

func main() {
	fmt.Println("Starting server...")

	if err := fasthttp.ListenAndServe("localhost:8080", fastHTTPHandler); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}
}
