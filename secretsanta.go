package main

import (
	"fmt"
	"math/rand"
	"bufio"
	"strings"
	"errors"
	"os"
	"net/smtp"
	"sync"
	"time"
	"sort"
	"strconv"
)

// Format of input file (named santa.txt):
//   Designated by four sections. Each section is defined by a set of newline-delimited data.
//   Each section ends with at least one empty line. In addition, the first section may optionally
//   be preceeded by an arbitrary amount of blank lines.
//
//   Section one:
//      Contains four lines:
//         1. Comma separated values for authentification of the sender of the emails:
//                 	identity,username,password,host
//              Note: Because it is csv list, none of the values may contain a comma
//         2. Comma separated values for email information:
//                   server,from
//         3. The subject of the email
//         4. A generic message for the email, where the following macros are supported:
//                   %1: Name of gift sender
//                   %2: Email of gift sender
//                   %3: Name of gift receiver
//   Section two:
//      Contains one line: the seed of the random matching. Must fit into a int64.
//   Section three:
//      A list of people in the secret santa in the following format:
//               name:emailaddress
//      Note: Names must not contain commas
//   Section four:
//      A list of sets of people who may not receive each other as matchings. Each set is
//      represented by a csv list. Note that names must be (almost) exactly the same as they
//      appear in section three. The almost comes from the fact that in this section, names
//      are run through strings.Title first.
//
//      This section is optional.

// If set to true, the emailer will instead send all of the matching emails to itself. Use
//   this to test that you set up section four correctly.
const DRY_RUN = false

type Input struct {
	sc *bufio.Scanner
	atLines bool
}

func (i*Input) Next() (string, bool) {
	for i.sc.Scan() {
		line := strings.Trim(i.sc.Text(), " \t")
		if strings.HasPrefix(line, "//") {
			continue
		}
		if len(line) > 0 {
			i.atLines = true
			return line, true
		}
		if i.atLines {
			i.atLines = false
			return "", false
		}
		// we weren't at lines already, so keep reading until we get some lines
	}
	return "", false
}

func (i*Input) Err() error {
	return i.sc.Err()
}

type Person struct {
	name string
	email string

	good []*Person
}

func (p*Person) String() string {
	return p.name
}

func readInPeople(in *Input) (map[string]*Person, error) {
	ret := make(map[string]*Person)
	for line, ok := in.Next(); ok; line, ok = in.Next(){
		colon := strings.Index(line, ":")
		if colon == -1 {
			return nil, errors.New(fmt.Sprint("Couldn't find email address in", line))
		}
		
		name, email := strings.Trim(line[:colon], " \t"), strings.Trim(line[colon+1:], " \t")
		
		ret[name] = &Person{
			name: name,
			email: email,
			good: nil,
		}
	}
	
	if err := in.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

type B struct{}

type BadSet struct {
	pbad map[*Person]B
}

func (b *BadSet) AddPerson(p *Person) {
	b.pbad[p] = B{}
}

func (b *BadSet) IsIn(p *Person) bool {
	_, ok := b.pbad[p]
	return ok
}

func (b *BadSet) AddAll(o *BadSet) {
	for p,_ := range o.pbad {
		b.pbad[p] = B{}
	}
}

func NewBadSet() *BadSet {
	return &BadSet {
		pbad: make(map[*Person]B),
	}
}

func (b *BadSet) String() string {
	return fmt.Sprint(b.pbad)
}

func readInBadSets(in *Input, people map[string]*Person) ([]*BadSet, error) {
	ret := make([]*BadSet, 0, 10)
	for line, ok := in.Next(); ok; line, ok = in.Next() {
		set := NewBadSet()
		ret = append(ret, set)
		
		for _, p := range strings.Split(line, ",") {
			p = strings.Title(p)
			set.AddPerson(people[p])
		}
	}
	if err := in.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func compileBadSet(bads []*BadSet) map[*Person]*BadSet {
	ret := make(map[*Person]*BadSet)
	for _,v := range bads {
		for p,_ := range v.pbad {
			b, ok := ret[p]
			if !ok {
				b = NewBadSet()
				ret[p] = b
			}
			b.AddAll(v)
		}
	}
	return ret
}

func makeDomain(cur *Person, all map[string]*Person, bad *BadSet) []*Person {
	ret := make([]*Person, 0, len(all)/2)
	for _,v := range all {
		if bad == nil {
			// Didn't belong to any badsets, so just make sure they can't get themself
			if v == cur {
				continue
			}
		} else if bad.IsIn(v) {
			// Only add them to the set if the person isn't in the badset
			continue
		}
		ret = append(ret, v)
	}
	return ret
}

type People []*Person

func (p People) Len() int { return len(p) }
func (p People) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p People) Less(i, j int) bool { return p[i].name < p[j].name }

func shuffle(l []*Person, r *rand.Rand) {
	// First sort them so that they are in a deterministic order so that the random assignment can be re-encountered if needed
	sort.Sort(People(l))
	for i := int32(len(l) - 1); i > 0; i-- {
		j := r.Int31n(i + 1)
		l[i], l[j] = l[j], l[i]
	}
}

func buildDomains(people map[string]*Person, bad []*BadSet, r *rand.Rand) []*Person {
	badMap := compileBadSet(bad)
	ret := make([]*Person, len(people))
	i := 0
	for _,v := range people {
		ret[i] = v
		i++
	}
	// Sort so that we have deterministic output
	sort.Sort(People(ret))
	for _,v := range ret {
		v.good = makeDomain(v, people, badMap[v])
		shuffle(v.good, r)
		fmt.Println("Domain for", v, ":", v.good)
	}
	return ret
}

func solve(all []*Person, cur int, assigned map[*Person]B) map[*Person]*Person {
	if cur == len(all) {
		// Got to the end without problems!
		return make(map[*Person]*Person, len(all))
	}
	
	c := all[cur]
	next := cur + 1
	for _,try := range c.good {
		if _,ok := assigned[try]; ok {
			// already assigned
			continue
		}
		
		// Try to assign this one
		assigned[try] = B{}
		if r := solve(all, next, assigned); r != nil {
			// This assignment was correct! Record our match and return
			r[c] = try
			return r
		} // else: guess caused problems. Try again.
		
		delete(assigned, try)
	}
	
	// Domain is zero - no solution!
	return nil
}


func makeRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

func main() {
	f, err := os.Open("santa.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	
	in := &Input{sc:bufio.NewScanner(f),}
	
	emailer,err := readInEmailer(in)
	if err != nil {
		panic(err)
	}
	
	var seed int64
	strSeed,_ := in.Next()
	if seed, err = strconv.ParseInt(strSeed, 10, 64); err != nil {
		panic(errors.New("Need a line designating the seed in the second section of the page" + err.Error()))
	}
	if _, b := in.Next(); b {
		panic(errors.New("Extra line at the end of the seed section"))
	}
	
	peoples, err := readInPeople(in)
	if err != nil {
		panic(err)
	}
	
	fmt.Println("Found", len(peoples), "people")
	
	rawbads, err := readInBadSets(in, peoples)
	if err != nil {
		panic(err)
	}
	
	people := buildDomains(peoples, rawbads, makeRand(seed))
	
	if DRY_RUN {
		fmt.Println("Doing a dry-run of matching!")
	} else {
		sc := bufio.NewScanner(os.Stdin)
		
		fmt.Println("WARNING: This is about to send out emails to everyone involved in the secret santa event. Please make sure that you have permission to email all of the people in the santa.txt file and that you are meaning to do this.")
		fmt.Println()
		fmt.Println("To continue press enter. To cancel press ctrl-c.")
		sc.Scan()
	}
	
	solution := solve(people, 0, make(map[*Person]B))
	if solution == nil {
		panic(errors.New("Could not find a solution with the given parameters!"))
	}
	
	// Shouldn't try to send too many emails at a time. Limit to four conncurrent sends.
	out, wait := BatchSend(4, *emailer)
	for k,v := range solution {
		out <- Assignment{k, v}
	}
	close(out)
	wait.Wait()
	
	fmt.Println("Done!")
}

func BatchSend(workers int, e Emailer) (chan<-Assignment, *sync.WaitGroup) {
	var done sync.WaitGroup
	done.Add(workers)
	
	ret := make(chan Assignment, 4)
	ffunc := func() {
		for ii := range ret {
			k, v := ii.src, ii.dest
			for {
				err := e.sendAssignment(k, v)
				if err != nil {
					fmt.Println("Error sending assignment to", k, ":", err)
					<-time.After(time.Second)
				} else {
					fmt.Println("Sent", k)//, ":", v)
					break
				}
			}
		}
		done.Done()
	}
	
	for i := 0; i < workers; i++ {
		go ffunc()
	}
	
	return ret, &done
}

type Assignment struct {
	src, dest *Person
}

func (e Emailer) sendAssignment(src, dest *Person) error {
	// %1: Name of gift sender
	// %2: Email of gift sender
	// %3: Name of gift receiver
	msg := strings.Replace(
				strings.Replace(
					strings.Replace(e.rawMsg, "%1", src.name, -1),
					"%2", src.email, -1),
				"%3", dest.name, -1)

	var to string
	if DRY_RUN {
		to = e.from
	} else {
		to = src.email
	}

	return smtp.SendMail(e.server, e.auth, e.from, []string{to}, ([]byte)(msg))
}

type Emailer struct {
	auth smtp.Auth
	server, from string
	rawMsg string
}

func readLine(in*Input, exp int, errString string) ([]string, error) {
	if l, b := in.Next(); !b {
		if e := in.Err(); e != nil {
			return nil, e
		}
	} else if exp == -1 {
		return []string{l}, nil
	} else if sp := strings.Split(l, ","); len(sp) == exp {
		return sp, nil
	}
	return nil, errors.New(errString)
}

func readInEmailer(in*Input) (*Emailer, error) {
	auths, err := readLine(in, 4, "Need four fields in the first line of the input file: identity,username,password,host")
	if err != nil {
		return nil, err
	}
	sp, err := readLine(in, 2, "Need two fields in the second line of the input file: server,from")
	if err != nil {
		return nil, err
	}
	subject, err := readLine(in, -1, "Need a line designating the subject in the third line of the file")
	if err != nil {
		return nil, err
	}
	msg, err := readLine(in, -1, "Need a line designating the generic message in the fourth line of the file")
	if err != nil {
		return nil, err
	}
	
	raw := "Subject: " + subject[0] + "\r\nContent-Type: text/html\r\n\r\n" + msg[0]
	
	if line, b := in.Next(); b {
		return nil, errors.New("Extra line at the end of the first section: " + line)
	}
	
	return &Emailer {
		auth: smtp.PlainAuth(auths[0], auths[1], auths[2], auths[3]),
		server: sp[0],
		from: sp[1],
		rawMsg: raw,
	}, nil
}






