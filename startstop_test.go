package startstop_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
)

type testLogger struct {
	debugs []string
	errors []string
}

func (t *testLogger) Debugf(f string, args ...interface{}) {
	t.debugs = append(t.debugs, fmt.Sprintf(f, args...))
}

func (t *testLogger) Errorf(f string, args ...interface{}) {
	t.errors = append(t.errors, fmt.Sprintf(f, args...))
}

type startStop struct {
	start func() error
	stop  func() error
}

func (s *startStop) Start() error {
	return s.start()
}

func (s *startStop) Stop() error {
	return s.stop()
}

type startStop2 struct {
	StartStop *startStop `inject:""`
	start     func() error
	stop      func() error
}

func (s *startStop2) Start() error {
	return s.start()
}

func (s *startStop2) Stop() error {
	return s.stop()
}

func TestStart(t *testing.T) {
	fin := make(chan struct{})
	obj := &startStop{
		start: func() error {
			defer close(fin)
			return nil
		},
	}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), nil))
	<-fin
}

func TestStop(t *testing.T) {
	fin := make(chan struct{})
	obj := &startStop{
		start: func() error { return nil },
		stop: func() error {
			defer close(fin)
			return nil
		},
	}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), nil))
	ensure.Nil(t, startstop.Stop(g.Objects(), nil))
	<-fin
}

func TestStartError(t *testing.T) {
	fin := make(chan struct{})
	actual := errors.New("err")
	obj := &startStop{
		start: func() error {
			defer close(fin)
			return actual
		},
	}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.DeepEqual(t, startstop.Start(g.Objects(), nil), actual)
	<-fin
}

func TestStopError(t *testing.T) {
	fin := make(chan struct{})
	actual := errors.New("err")
	obj := &startStop{
		start: func() error { return nil },
		stop: func() error {
			defer close(fin)
			return actual
		},
	}
	logger := &testLogger{}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), logger))
	ensure.DeepEqual(t, startstop.Stop(g.Objects(), logger), actual)
	ensure.DeepEqual(t, logger.debugs, []string{
		"starting *startstop_test.startStop",
		"stopping *startstop_test.startStop",
	})
	ensure.DeepEqual(t, logger.errors, []string{"error stopping *startstop_test.startStop: err"})
	<-fin
}

func TestStartOrder(t *testing.T) {
	res := make(chan int, 2)
	obj1 := &startStop{
		start: func() error {
			defer func() { res <- 1 }()
			return nil
		},
	}
	obj2 := &startStop2{
		start: func() error {
			defer func() { res <- 2 }()
			return nil
		},
	}

	var g inject.Graph
	ensure.Nil(
		t,
		g.Provide(
			&inject.Object{Value: obj1},
			&inject.Object{Value: obj2},
		),
	)
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), nil))
	ensure.DeepEqual(t, <-res, 1)
	ensure.DeepEqual(t, <-res, 2)
}

type openClose struct {
	open  func() error
	close func() error
}

func (s *openClose) Open() error {
	return s.open()
}

func (s *openClose) Close() error {
	return s.close()
}

func TestOpen(t *testing.T) {
	fin := make(chan struct{})
	obj := &openClose{
		open: func() error {
			defer close(fin)
			return nil
		},
	}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), nil))
	<-fin
}

func TestClose(t *testing.T) {
	fin := make(chan struct{})
	obj := &openClose{
		open: func() error { return nil },
		close: func() error {
			defer close(fin)
			return nil
		},
	}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), nil))
	ensure.Nil(t, startstop.Stop(g.Objects(), nil))
	<-fin
}

func TestOpenError(t *testing.T) {
	fin := make(chan struct{})
	actual := errors.New("err")
	obj := &openClose{
		open: func() error {
			defer close(fin)
			return actual
		},
	}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.DeepEqual(t, startstop.Start(g.Objects(), nil), actual)
	<-fin
}

func TestCloseError(t *testing.T) {
	fin := make(chan struct{})
	actual := errors.New("err")
	obj := &openClose{
		open: func() error { return nil },
		close: func() error {
			defer close(fin)
			return actual
		},
	}
	logger := &testLogger{}

	var g inject.Graph
	ensure.Nil(t, g.Provide(&inject.Object{Value: obj}))
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), logger))
	ensure.DeepEqual(t, startstop.Stop(g.Objects(), logger), actual)
	ensure.DeepEqual(t, logger.debugs, []string{
		"opening *startstop_test.openClose",
		"closing *startstop_test.openClose",
	})
	ensure.DeepEqual(t, logger.errors, []string{"error closing *startstop_test.openClose: err"})
	<-fin
}

type caseStartStop struct {
	Name      string
	ValidCase *ValidCase
}

func (c *caseStartStop) Start() error {
	c.ValidCase.mutex.Lock()
	defer c.ValidCase.mutex.Unlock()
	c.ValidCase.actualStart = append(c.ValidCase.actualStart, c.Name)
	return nil
}

func (c *caseStartStop) Stop() error {
	c.ValidCase.mutex.Lock()
	defer c.ValidCase.mutex.Unlock()
	c.ValidCase.actualStop = append(c.ValidCase.actualStop, c.Name)
	return nil
}

type ValidCase struct {
	T           *testing.T
	Graph       map[string][]string
	Expected    [][]string
	actualStart []string
	actualStop  []string
	mutex       sync.Mutex
}

func (c *ValidCase) Objects() []*inject.Object {
	hasStartStop := map[string]bool{}
	objectMap := map[string]*inject.Object{}
	var objects []*inject.Object

	// figure out which nodes have start/stop
	for _, batch := range c.Expected {
		for _, name := range batch {
			if hasStartStop[name] {
				c.T.Fatalf("%s shows up twice in Expected", name)
			}
			hasStartStop[name] = true
		}
	}

	// make all the nodes
	for name := range c.Graph {
		var value interface{}
		if hasStartStop[name] {
			value = &caseStartStop{
				Name:      name,
				ValidCase: c,
			}
		} else {
			value = struct{}{}
		}

		o := &inject.Object{
			Value:  value,
			Fields: map[string]*inject.Object{},
		}
		objectMap[name] = o
		objects = append(objects, o)
	}

	// connect all the nodes
	for name, connections := range c.Graph {
		o := objectMap[name]
		for _, depName := range connections {
			dep := objectMap[depName]
			ensure.True(
				c.T,
				dep != nil,
				fmt.Sprintf("misconfigured test - did not find dep %s in graph", depName),
			)
			o.Fields[depName] = dep
		}
	}

	return objects
}

func (c *ValidCase) Run() {
	objects := c.Objects()
	ensure.Nil(c.T, startstop.Start(objects, nil))
	ensure.Nil(c.T, startstop.Stop(objects, nil))

	// make a reverseStop to make comparing the expected results easier
	reverseStop := make([]string, len(c.actualStop))
	reverseIndex := 0
	for i := len(c.actualStop) - 1; i >= 0; i-- {
		reverseStop[reverseIndex] = c.actualStop[i]
		reverseIndex++
	}

	// ensure we started & stopped in the expected order
	index := 0
	for _, batch := range c.Expected {
		ensure.SameElements(c.T, c.actualStart[index:index+len(batch)], batch)
		ensure.SameElements(c.T, reverseStop[index:index+len(batch)], batch)
		index = index + len(batch)
	}
	ensure.DeepEqual(c.T, index, len(c.actualStart))
	ensure.DeepEqual(c.T, index, len(c.actualStop))
}

// A  →  B
//  ↖   ↙
//    C
func TestTriangleWithNoStart(t *testing.T) {
	(&ValidCase{
		T: t,
		Graph: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
	}).Run()
}

// (A)  →  B
//   ↖   ↙
//     C
func TestTriangleWithOneStart(t *testing.T) {
	(&ValidCase{
		T:        t,
		Expected: [][]string{{"A"}},
		Graph: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
	}).Run()
}

// A ↔ B
func TestPairNoStart(t *testing.T) {
	(&ValidCase{
		T: t,
		Graph: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}).Run()
}

// (A) ↔ B
func TestPairOneStart(t *testing.T) {
	(&ValidCase{
		T:        t,
		Expected: [][]string{{"A"}},
		Graph: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}).Run()
}

//     (B)
//   ↗    ↘
// (A)     (C)
//   ↘    ↗
//      D
func TestDiamond(t *testing.T) {
	(&ValidCase{
		T:        t,
		Expected: [][]string{{"C"}, {"B"}, {"A"}},
		Graph: map[string][]string{
			"A": {"B", "D"},
			"B": {"C"},
			"C": nil,
			"D": {"C"},
		},
	}).Run()
}

//    (B)
//  ↗
// A → (C) → (E)
//  ↘
//    (D)
func TestFan(t *testing.T) {
	(&ValidCase{
		T:        t,
		Expected: [][]string{{"B", "D", "E"}, {"C"}},
		Graph: map[string][]string{
			"A": {"B", "C", "D"},
			"B": nil,
			"C": {"E"},
			"D": nil,
			"E": nil,
		},
	}).Run()
}

//    B   (F)
//  ↗    ↙
// A ↔  (C) → E
//  ↘         ↓
//    D      (G)
func TestComplexOne(t *testing.T) {
	(&ValidCase{
		T:        t,
		Expected: [][]string{{"G"}, {"C"}, {"F"}},
		Graph: map[string][]string{
			"A": {"B", "C", "D"},
			"B": nil,
			"C": {"E"},
			"D": nil,
			"E": {"G"},
			"F": {"C"},
			"G": nil,
		},
	}).Run()
}

//     B
//   ↗
// (A) ↔  C
//   ↘
//     D  → (E)
func TestComplexTwo(t *testing.T) {
	(&ValidCase{
		T:        t,
		Expected: [][]string{{"E"}, {"A"}},
		Graph: map[string][]string{
			"A": {"B", "C", "D"},
			"B": nil,
			"C": {"A"},
			"D": {"E"},
			"E": nil,
		},
	}).Run()
}

type InvalidCase struct {
	T        *testing.T
	Graph    map[string][]string
	Eligible []string
	Message  string
}

func (c *InvalidCase) Objects() []*inject.Object {
	hasStartStop := map[string]bool{}
	objectMap := map[string]*inject.Object{}
	var objects []*inject.Object

	// map out which nodes have start/stop
	for _, name := range c.Eligible {
		if hasStartStop[name] {
			c.T.Fatalf("%s shows up twice in Expected", name)
		}
		hasStartStop[name] = true
	}

	// make all the nodes
	for name := range c.Graph {
		var value interface{}
		if hasStartStop[name] {
			value = &startStop{
				start: func() error { c.T.Fatal("should not get called"); return nil },
				stop:  func() error { c.T.Fatal("should not get called"); return nil },
			}
		} else {
			value = struct{}{}
		}

		o := &inject.Object{
			Value:  value,
			Fields: map[string]*inject.Object{},
		}
		objectMap[name] = o
		objects = append(objects, o)
	}

	// connect all the nodes
	for name, connections := range c.Graph {
		o := objectMap[name]
		for _, depName := range connections {
			dep := objectMap[depName]
			ensure.True(
				c.T,
				dep != nil,
				fmt.Sprintf("misconfigured test - did not find dep %s in graph", depName),
			)
			o.Fields[depName] = dep
		}
	}

	return objects
}

func (c *InvalidCase) Run() {
	objects := c.Objects()

	err := startstop.Start(objects, nil)
	ensure.NotNil(c.T, err)
	c.EnsureExpectedCycle(err)

	err = startstop.Stop(objects, nil)
	ensure.NotNil(c.T, err)
	c.EnsureExpectedCycle(err)
}

func (c *InvalidCase) EnsureExpectedCycle(e error) {
	actualParts := strings.Split(e.Error(), "\n")
	// drop last repeat part if not a cycle to it self
	if len(actualParts) > 1 {
		actualParts = actualParts[0 : len(actualParts)-1]
	}

	expectedParts := strings.Split(c.Message, "\n")
	// drop last repeat part if not a cycle to it self
	if len(expectedParts) > 1 {
		expectedParts = expectedParts[0 : len(expectedParts)-1]
	}

	ensure.SameElements(c.T, actualParts, expectedParts)
}

// A ↔ B
func TestCodependentPair(t *testing.T) {
	(&InvalidCase{
		T:        t,
		Eligible: []string{"A", "B"},
		Graph: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Message: `circular reference detected from
field A in <nil>
field B in <nil>
field A in <nil>`,
	}).Run()
}

// (A)  →   (B)
//   ↖     ↙
//      C
func TestTriangleWithTwoStarts(t *testing.T) {
	(&InvalidCase{
		T:        t,
		Eligible: []string{"A", "B"},
		Graph: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
		Message: `circular reference detected from
field A in <nil>
field B in <nil>
field C in <nil>
field A in <nil>`,
	}).Run()
}

// (A) → (B)
//  ↑     ↓
//  D  ←  C
func TestSquareWithTwoStarts(t *testing.T) {
	(&InvalidCase{
		T:        t,
		Eligible: []string{"A", "B"},
		Graph: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"D"},
			"D": {"A"},
		},
		Message: `circular reference detected from
field A in <nil>
field B in <nil>
field C in <nil>
field D in <nil>
field A in <nil>`,
	}).Run()
}

// (A) ↩
func TestSelfDependent(t *testing.T) {
	(&InvalidCase{
		T:        t,
		Eligible: []string{"A"},
		Graph: map[string][]string{
			"A": {"A"},
		},
		Message: "circular reference detected from field A in <nil> to itself",
	}).Run()
}

type startButNoStop struct {
	start func() error
}

func (s *startButNoStop) Start() error {
	return s.start()
}

type stopButNoStart struct {
	stop func() error
}

func (s *stopButNoStart) Stop() error {
	return s.stop()
}

type openButNoClose struct {
	open func() error
}

func (s *openButNoClose) Open() error {
	return s.open()
}

type closeButNoOpen struct {
	close func() error
}

func (s *closeButNoOpen) Close() error {
	return s.close()
}

func TestOneHalfOnly(t *testing.T) {
	res := make(chan int, 4)
	var g inject.Graph
	ensure.Nil(
		t,
		g.Provide(
			&inject.Object{
				Value: &startButNoStop{
					start: func() error {
						defer func() { res <- 1 }()
						return nil
					},
				},
			},
			&inject.Object{
				Value: &openButNoClose{
					open: func() error {
						defer func() { res <- 1 }()
						return nil
					},
				},
			},
			&inject.Object{
				Value: &stopButNoStart{
					stop: func() error {
						defer func() { res <- 2 }()
						return nil
					},
				},
			},
			&inject.Object{
				Value: &closeButNoOpen{
					close: func() error {
						defer func() { res <- 2 }()
						return nil
					},
				},
			},
		),
	)
	ensure.Nil(t, g.Populate())
	ensure.Nil(t, startstop.Start(g.Objects(), nil))
	ensure.Nil(t, startstop.Stop(g.Objects(), nil))
	ensure.DeepEqual(t, <-res, 1)
	ensure.DeepEqual(t, <-res, 1)
	ensure.DeepEqual(t, <-res, 2)
	ensure.DeepEqual(t, <-res, 2)
}
