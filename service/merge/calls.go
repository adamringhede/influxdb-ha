package merge

import "strconv"

type Expr struct {
	Field string
}

func (n *Expr) Next(source ResultSource) []float64 {
	return source.Next(n.Field)
}

type MovingAverage struct {

}

func (n *MovingAverage) Values() []float64 {
	// get raw results and just do a weighted mean of them.
	return []float64{}
}

type Top struct {
	tops *Expr
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

type Mean struct {
	sums *Expr
	counts *Expr
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
	var weightedSum float64
	var total float64
	for i, sum := range sums {
		weightedSum += sum * counts[i]
		total += counts[i]
	}
	return []float64{weightedSum / total}
}

type Max struct {
	maxes *Expr
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
	sums *Expr
}

func NewSum(fieldKey string, qb *QueryBuilder) *Sum {
	return &Sum{qb.Get("sum(" + fieldKey + ")")}
}

func (n *Sum) Next(source ResultSource) []float64 {
	sum := 0.0
	for _, v := range n.sums.Next(source) {
		sum += v
	}
	return []float64{sum}
}