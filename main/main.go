package main

import (
	"fmt"
	"log"

	"encoding/json"
	"os"
	"sync"

	"bytes"

	"time"

	"github.com/bradfitz/slice"
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
		{path + "visits_1.json", FillVisMap},
		{path + "locations_1.json", FillLocMap},
		{path + "users_1.json", FillUsMap},
	} {
		go func(filename string, fill func(decoder *json.Decoder)) {
			defer wg.Done()
			f, err := os.OpenFile(filename, os.O_RDONLY, 0)
			if err != nil {
				panic(err)
			}
			defer f.Close()

			dec := json.NewDecoder(f)
			dec.Token()
			dec.Token()
			dec.Token()
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
var VisitsKeyWord = []byte("visits")
var AvgKeyWord = []byte("avg")

func fastHTTPHandler(ctx *fasthttp.RequestCtx) {

	p := bytes.Split(ctx.RequestURI(), []byte{'/'})

	switch {
	case len(p) == 4 && bytes.Equal(p[1], EntityLocations) && bytes.Equal(p[3], AvgKeyWord):
		id := intFromBytes(p[2])
		GetLocationsAVGRH(id, ctx)

	case len(p) == 4 && bytes.Equal(p[1], EntityUsers) && bytes.Equal(p[3], VisitsKeyWord):
		id := intFromBytes(p[2])
		GetUserVisistsRH(id, ctx)

	case len(p) == 3 && (bytes.Equal(p[1], EntityUsers) || bytes.Equal(p[1], EntityVisits) || bytes.Equal(p[1], EntityLocations)):
		id := intFromBytes(p[2])
		ctx.SetUserValue("entity", string(p[1]))
		GetEntityRH(id, ctx)

	default:
		//		fmt.Fprintf(ctx, "Hi there! RequestURI is %q", ctx.RequestURI())
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	}

}

func intFromBytes(b []byte) (res int) {

	if len(b) == 0 {
		return -1
	}

	var base = 1
	for i := len(b) - 1; i >= 0; i-- {
		if !('0' <= b[i] && b[i] <= '9') {
			res = -1
			break
		}

		res += int(b[i]-'0') * base
		base *= 10
	}

	return
}

var SrvAddr = "localhost:80"

// GET <entity>/users
// POST <entity>/<new>
// GET locations/<id>/avg
// GET users/<id>/visits

func GetEntityRH(id int, ctx *fasthttp.RequestCtx) {
	var entity interface{}
	var ok bool

	switch ctx.UserValue("entity") {
	case "users":
		entity, ok = UserMap[id]
	case "locations":
		entity, ok = LocMap[id]
	case "visits":
		entity, ok = VisMap[id]
	}

	if ok {
		buf, err := json.Marshal(entity)
		if err != nil {
			panic(err)
		}
		ctx.SetStatusCode(fasthttp.StatusOK)
		fmt.Fprintf(ctx, "%s", buf)
	} else {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	}
}

func parseArg(args *fasthttp.Args, names []string, vals []*int) bool {
	// len names == len vals

	var tmp []byte
	for k, v := range names {
		if !args.Has(v) {
			continue
		}

		if tmp = args.Peek(v); tmp != nil {
			*vals[k] = intFromBytes(tmp)
			if *vals[k] == -1 {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

func GetUserVisistsRH(uId int, ctx *fasthttp.RequestCtx) {

	args := ctx.QueryArgs()

	var from, to, toDistance int = -1, -1, -1
	if !parseArg(args,
		[]string{"fromDate", "toDate", "toDistance"},
		[]*int{&from, &to, &toDistance},
	) {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	var country string
	if tmp := args.Peek("country"); tmp != nil {
		country = string(tmp)
	}

	if _, ok := UserMap[uId]; !ok {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	type UVRes struct {
		visitedAt, mark int
		place           string
	}

	resArr := []*UVRes{}
	for _, v := range VisMap {
		if v.User != uId {
			continue
		}
		if from >= 0 && v.VisitedAt <= from {
			continue
		}
		if to >= 0 && v.VisitedAt >= to {
			continue
		}

		loc := LocMap[v.Location]

		if toDistance > -1 && loc.Distance >= toDistance {
			continue
		}

		if country != "" && country != loc.Country {
			continue
		}

		resArr = append(resArr, &UVRes{
			mark:      v.Mark,
			place:     loc.Place,
			visitedAt: v.VisitedAt,
		})
	}

	slice.Sort(resArr, func(i, j int) bool {
		return resArr[i].visitedAt < resArr[j].visitedAt
	})

	fmt.Fprintf(ctx, `{ "visits" : [`+"\n")

	l := len(resArr)
	for i := 0; i < l-1; i++ {
		v := resArr[i]
		fmt.Fprintf(ctx, `{"visitedAt": %d, "mark": %d, "place": "%s"},`+"\n", v.visitedAt, v.mark, v.place)
	}

	if l > 0 {
		v := resArr[l-1]
		fmt.Fprintf(ctx, `{"visitedAt": %d, "mark": %d, "place": "%s"}`+"\n", v.visitedAt, v.mark, v.place)
	}

	fmt.Fprintf(ctx, `]}`)
}

func GetLocationsAVGRH(locId int, ctx *fasthttp.RequestCtx) {

	args := ctx.QueryArgs()

	var from, to, fromAge, toAge int = -1, -1, -1, -1
	if !parseArg(args,
		[]string{"fromDate", "toDate", "fromAge", "toAge"},
		[]*int{&from, &to, &fromAge, &toAge},
	) {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	if _, ok := LocMap[locId]; !ok {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	// make sum in different goroutines?
	var sum int = 0
	var count int = 0
	for _, v := range VisMap {
		if v.Location != locId {
			continue
		}

		if from > -1 && v.VisitedAt <= from {
			continue
		}
		if to > -1 && v.VisitedAt >= to {
			continue
		}

		usr := UserMap[v.User]

		if fromAge > -1 && int64(usr.BirthDate) >= time.Now().AddDate(-fromAge, 0, 0).Unix() {
			continue
		}

		if toAge > -1 && time.Now().AddDate(-toAge, 0, 0).Unix() <= int64(usr.BirthDate) {
			continue
		}

		sum += v.Mark
		count++
	}

	if count == 0 {
		fmt.Fprintf(ctx, "0")
	} else {
		fmt.Fprintf(ctx, "%0.3f", float32(sum)/float32(count))
	}

}

func main() {
	fmt.Println("Starting server " + SrvAddr)

	//fhhp := fasthttprouter.New()
	//
	//fhhp.GET("/users/:id/visits", GetUserVisistsRH)
	//fhhp.GET("/locations/:id/avg", GetLocationsAVGRH)
	//
	//fhhp.GET("/users/:id", GetEntityRH)
	//fhhp.GET("/locations/:id", GetEntityRH)
	//fhhp.GET("/visits/:id", GetEntityRH)

	//if err := fasthttp.ListenAndServe(SrvAddr, fhhp.Handler); err != nil {
	//	log.Fatalf("error in ListenAndServe: %s", err)
	//}

	if err := fasthttp.ListenAndServe(SrvAddr, fastHTTPHandler); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}
}
