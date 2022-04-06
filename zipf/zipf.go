package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
)

const (
	// ZipfianConstant is the default constant for the zipfian.
	ZipfianConstant = float64(0.99)
)

type Zipfian struct {
	items int64
	base  int64

	zipfianConstant float64

	alpha      float64
	zetan      float64
	theta      float64
	eta        float64
	zeta2Theta float64

	countForZeta int64

	allowItemCountDecrease bool
}

// NewZipfianWithItems creates the Zipfian generator.
func NewZipfianWithItems(items int64, zipfianConstant float64) *Zipfian {
	return NewZipfianWithRange(0, items-1, zipfianConstant)
}

// NewZipfianWithRange creates the Zipfian generator.
func NewZipfianWithRange(min int64, max int64, zipfianConstant float64) *Zipfian {
	return NewZipfian(min, max, zipfianConstant, zetaStatic(0, max-min+1, zipfianConstant, 0))
}

// NewZipfian creates the Zipfian generator.
func NewZipfian(min int64, max int64, zipfianConstant float64, zetan float64) *Zipfian {
	items := max - min + 1
	z := new(Zipfian)

	z.items = items
	z.base = min

	z.zipfianConstant = zipfianConstant
	theta := z.zipfianConstant
	z.theta = theta

	z.zeta2Theta = z.zeta(0, 2, theta, 0)

	z.alpha = 1.0 / (1.0 - theta)
	z.zetan = zetan
	z.countForZeta = items
	z.eta = (1 - math.Pow(2.0/float64(items), 1-theta)) / (1 - z.zeta2Theta/z.zetan)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z.Next(r)
	return z
}

func (z *Zipfian) zeta(st int64, n int64, thetaVal float64, initialSum float64) float64 {
	z.countForZeta = n
	return zetaStatic(st, n, thetaVal, initialSum)
}

func zetaStatic(st int64, n int64, theta float64, initialSum float64) float64 {
	sum := initialSum

	for i := st; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}

	return sum
}

func (z *Zipfian) next(r *rand.Rand, itemCount int64) int64 {
	if itemCount != z.countForZeta {
		if itemCount > z.countForZeta {
			//we have added more items. can compute zetan incrementally, which is cheaper
			z.zetan = z.zeta(z.countForZeta, itemCount, z.theta, z.zetan)
			z.eta = (1 - math.Pow(2.0/float64(z.items), 1-z.theta)) / (1 - z.zeta2Theta/z.zetan)
		} else if itemCount < z.countForZeta && z.allowItemCountDecrease {
			//note : for large itemsets, this is very slow. so don't do it!
			fmt.Printf("recomputing Zipfian distribution, should be avoided,item count %v, count for zeta %v\n", itemCount, z.countForZeta)
			z.zetan = z.zeta(0, itemCount, z.theta, 0)
			z.eta = (1 - math.Pow(2.0/float64(z.items), 1-z.theta)) / (1 - z.zeta2Theta/z.zetan)
		}
	}

	u := r.Float64()
	uz := u * z.zetan

	if uz < 1.0 {
		return z.base
	}

	if uz < 1.0+math.Pow(0.5, z.theta) {
		return z.base + 1
	}

	ret := z.base + int64(float64(itemCount)*math.Pow(z.eta*u-z.eta+1, z.alpha))
	return ret
}

// Next implements the Generator Next interface.
func (z *Zipfian) Next(r *rand.Rand) int64 {
	return z.next(r, z.items)
}

var cfgSkewness float64
var cfgItems int

type occurrence struct {
	index int
	count int
}

type occurrences []occurrence

func (p occurrences) Len() int {
	return len(p)
}

func (p occurrences) Less(i, j int) bool {
	return p[i].count > p[j].count
}

func (p occurrences) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func main() {
	flag.Float64Var(&cfgSkewness, "skewness", 0.5, "theta of zipf distribution")
	flag.IntVar(&cfgItems, "items", 0x1000, "number of keys for request")
	flag.Parse()

	zipfGen := NewZipfianWithItems(int64(cfgItems), cfgSkewness)
	ran := rand.New(rand.NewSource(0))

	ranAsso := make(map[int]int) // mapping from 0~cfgItems to random keys
	occRan := rand.New(rand.NewSource(time.Now().UnixNano()))
	newIdx := 0
	for {
		tryIdx := occRan.Intn(cfgItems)
		if len(ranAsso) == cfgItems {
			break
		}
		if _, ok := ranAsso[tryIdx]; ok {
			continue
		}
		ranAsso[tryIdx] = newIdx
		newIdx += 1
	}

	total := 100000
	newSeq := make([]int, total)
	order := make([]int, cfgItems)
	for i := 0; i < total; i++ {
		num := zipfGen.Next(ran)
		// log.Printf("num = %v", num)
		newNum := ranAsso[int(num)]
		order[newNum] += 1
		newSeq[i] = newNum
	}
	for i := range order {
		log.Printf("i %v: %v", i, order[i])
	}

	// occ := make([]occurrence, cfgItems)

	// sort.Sort(occurrences(occ))
	// for i := range occ {
	// 	log.Printf("i %v: %v", i, occ[i])
	// }

}
