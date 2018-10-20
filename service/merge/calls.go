package merge

import (
	"fmt"
	"math"
	"strconv"
)

type Values struct {
	Field string
}

func (n *Values) Next(source ResultSource) []float64 {
	return source.Next(n.Field)
}

type MovingAverage struct {
	values *Values
	counts *Values
}

func NewMovingAverage(fieldKey string, n int, qb *QueryBuilder) *MovingAverage {
	return &MovingAverage{
		qb.Get("moving_average(" + fieldKey + ", " + strconv.Itoa(n) + ")"),
		qb.Get("count(" + fieldKey + ")"),
	}
}

func (n *MovingAverage) Next(source ResultSource) []float64 {
	values := n.values.Next(source)
	counts := n.counts.Next(source)
	var totalSum float64
	var totalCount float64
	for i, value := range values {
		if counts[i] > 0 {
			totalSum += value * counts[i]
			totalCount += counts[i]
		}
	}
	return []float64{totalSum / totalCount}
}

type Percentile struct {
	percentiles *Values
	counts *Values
}

func NewPercentile(fieldKey string, p float64, qb *QueryBuilder) *Percentile {
	return &Percentile{
		qb.Get(fmt.Sprintf("percentile(%s, %f.5)", fieldKey, p)),
		qb.Get("count(" + fieldKey + ")"),
	}
}

func (n *Percentile) Next(source ResultSource) []float64 {
	values := n.percentiles.Next(source)
	counts := n.counts.Next(source)
	var totalSum float64
	var totalCount float64
	for i, value := range values {
		if counts[i] > 0 {
			totalSum += value * counts[i]
			totalCount += counts[i]
		}
	}
	return []float64{totalSum / totalCount}
}

type Median struct {
	medians *Values
	counts *Values
}


func NewMedian(fieldKey string, qb *QueryBuilder) *Percentile {
	return &Percentile{
		qb.Get(fmt.Sprintf("median(%s)", fieldKey)),
		qb.Get("count(" + fieldKey + ")"),
	}
}

func (n *Median) Next(source ResultSource) []float64 {
	values := n.medians.Next(source)
	counts := n.counts.Next(source)
	var totalSum float64
	var totalCount float64
	for i, value := range values {
		if counts[i] > 0 {
			totalSum += value * counts[i]
			totalCount += counts[i]
		}
	}
	return []float64{totalSum / totalCount}
}

type Stddev struct {
	stds *Values
	counts *Values
}


func NewStddev(fieldKey string, qb *QueryBuilder) *Stddev {
	return &Stddev{
		qb.Get(fmt.Sprintf("stddev(%s)", fieldKey)),
		qb.Get("count(" + fieldKey + ")"),
	}
}

func (n *Stddev) Next(source ResultSource) []float64 {
	values := n.stds.Next(source)
	counts := n.counts.Next(source)
	var totalSum float64
	var totalCount float64
	for i, value := range values {
		if counts[i] > 0 {
			totalSum += value * counts[i]
			totalCount += counts[i]
		}
	}
	return []float64{totalSum / totalCount}
}


type Sample struct {
	values *Values
	count int
}

func NewSample(fieldKey string, count int, qb *QueryBuilder) *Sample {
	return &Sample{
		qb.Get("sample(" + fieldKey + ", " + strconv.Itoa(count) + ")"),
		count,
	}
}

func (n *Sample) Next(source ResultSource) []float64 {
	samples := n.values.Next(source)
	return samples[:n.count]
}

type Top struct {
	tops *Values
	count int
}

func NewTop(fieldKey string, count int, qb *QueryBuilder) *Top {
	return &Top{
		qb.Get("top(" + fieldKey + ", " + strconv.Itoa(count) + ")"),
		count,
	}
}

func (n *Top) Next(source ResultSource) []float64 {
	tops := n.tops.Next(source)
	for i := 0; i < n.count; i++ {
		maxIndex := i
		maxValue := tops[i]
		for j := i + 1; j < len(tops); j++ {
			if tops[j] > maxValue {
				maxIndex = j
				maxValue = tops[j]
				tops[i], tops[maxIndex] = tops[maxIndex], tops[i]
			}
		}
	}
	return tops[:n.count]
}

type Bottom struct {
	bottoms *Values
	count int
}

func NewBottom(fieldKey string, count int, qb *QueryBuilder) *Bottom {
	return &Bottom{
		qb.Get("bottom(" + fieldKey + ", " + strconv.Itoa(count) + ")"),
		count,
	}
}

func (n *Bottom) Next(source ResultSource) []float64 {
	bottoms := n.bottoms.Next(source)
	for i := 0; i < n.count; i++ {
		minIndex := i
		minValue := bottoms[i]
		for j := i + 1; j < len(bottoms); j++ {
			if bottoms[j] < minValue {
				minIndex = j
				minValue = bottoms[j]
				bottoms[i], bottoms[minIndex] = bottoms[minIndex], bottoms[i]
			}
		}
	}
	return bottoms[:n.count]
}

type Spread struct {
	maxs *Values
	mins *Values
}

func NewSpread(fieldKey string, qb *QueryBuilder) *Spread {
	return &Spread{
		qb.Get("max(" + fieldKey + ")"),
		qb.Get("min(" + fieldKey + ")"),
	}
}

func (n *Spread) Next(source ResultSource) []float64 {
	var max = -math.MaxFloat64
	for _, v := range n.maxs.Next(source) {
		if v > max {
			max = v
		}
	}
	var min = math.MaxFloat64
	for _, v := range n.mins.Next(source) {
		if v < min {
			max = v
		}
	}
	return []float64{max-min}
}

type Distinct struct {
	distinct *Values
}

func NewDistinct(fieldKey string, qb *QueryBuilder) *Distinct {
	return &Distinct{
		qb.Get("distinct(" + fieldKey + ")"),
	}
}

func (n *Distinct) Next(source ResultSource) []float64 {
	unique := map[float64]bool{}
	for _, v := range n.distinct.Next(source) {
		unique[v] = true
	}
	distinct := make([]float64, len(unique))
	for v := range unique {
		distinct = append(distinct, v) // FIXME Don't use append as the result will be longer then len(unique)
	}
	return distinct
}

type Mean struct {
	sums *Values
	counts *Values
}

func NewMean(fieldKey string, qb *QueryBuilder) *Mean {
	return &Mean{
		qb.Get("sum(" + fieldKey + ")"),
		qb.Get("count(" + fieldKey + ")"),
	}
}

func (n *Mean) Next(source ResultSource) []float64 {
	sums := n.sums.Next(source)
	counts := n.counts.Next(source)
	var totalSum float64
	var totalCount float64
	for i, sum := range sums {
		totalSum += sum
		totalCount += counts[i]
	}
	return []float64{totalSum / totalCount}
}

type Mode struct {
	modes *Values
}

func NewMode(fieldKey string, qb *QueryBuilder) *Mode {
	return &Mode{
		qb.Get("mode(" + fieldKey + ")"),
	}
}

func (n *Mode) Next(source ResultSource) []float64 {
	modes := n.modes.Next(source)
	m := map[float64]int{}
	maxCount := 0
	var mode float64
	for _, value := range modes {
		if _, ok := m[value]; !ok {
			m[value] = 0
		}
		m[value] += 1
		if m[value] > maxCount {
			mode, maxCount = value, m[value]
		}
	}
	return []float64{mode}
}

type Count struct {
	counts *Values
}

func NewCount(fieldKey string, qb *QueryBuilder) *Count {
	return &Count{
		qb.Get("count(" + fieldKey + ")"),
	}
}

func (n *Count) Next(source ResultSource) []float64 {
	var sum float64
	for _, v := range n.counts.Next(source) {
		sum += v
	}
	return []float64{sum}
}

type Max struct {
	maxes *Values
}

func (n *Max) Next(source ResultSource) []float64 {
	var max float64
	for _, v := range n.maxes.Next(source) {
		if v > max {
			max = v
		}
	}
	return []float64{max}
}

type Sum struct {
	sums *Values
}

func NewSum(fieldKey string, qb *QueryBuilder) *Sum {
	return &Sum{qb.Get("sum(" + fieldKey + ")")}
}

func (n *Sum) Next(source ResultSource) []float64 {
	var sum float64 = 0
	for _, v := range n.sums.Next(source) {
		sum += v
	}
	return []float64{sum}
}