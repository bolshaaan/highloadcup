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

var (
	VisMap  map[int]*Visit    = make(map[int]*Visit)
	LocMap  map[int]*Location = make(map[int]*Location)
	UserMap map[int]*User     = make(map[int]*User)
)

func FillUsMap(dec *json.Decoder) {
	for dec.More() {
		newV := &User{}
		if err := dec.Decode(newV); err != nil {
			panic(err)
		}
		UserMap[newV.ID] = newV
	}
}
func FillLocMap(dec *json.Decoder) {
	for dec.More() {
		newV := &Location{}
		if err := dec.Decode(newV); err != nil {
			panic(err)
		}
		LocMap[newV.ID] = newV
	}
}

func FillVisMap(dec *json.Decoder) {
	for dec.More() {
		newV := &Visit{}
		if err := dec.Decode(newV); err != nil {
			panic(err)
		}
		VisMap[newV.ID] = newV
	}
}

// var path = "/Users/aleksandr/hlcupdocs/data/TRAIN/data/"
var path string = `D:\ub_shared\hlcupdocs\data\TRAIN\data\`

func init() {
	wg := sync.WaitGroup{}
	wg.Add(3)
	for _, r := range []struct {
		file string
		fill func(decoder *json.Decoder)
	}{
		{path + "visits_1.json",  FillVisMap},
		{path + "locations_1.json",FillLocMap},
		{path + "users_1.json",  FillUsMap},
	} {
		go func(filename string, fill func(decoder *json.Decoder)) {
			defer wg.Done()
			f, err := os.OpenFile(filename, os.O_RDONLY, 0)
			if err != nil {
				panic(err)
			}
			defer f.Close()

			dec := json.NewDecoder(f)
			dec.Token(); dec.Token(); dec.Token()
			fill(dec)
			//if err := decoder.Decode(d); err != nil {
			//	panic(err)
			//}
		}(r.file, r.fill)
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
	case bytes.Equal(p[1], EntityUsers),
		 bytes.Equal(p[1], EntityLocations),
		 bytes.Equal(p[1], EntityVisits):

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

var SrvAddr = "localhost:80"

func main() {
	fmt.Println("Starting server " + SrvAddr)

	if err := fasthttp.ListenAndServe(SrvAddr, fastHTTPHandler); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}
}
