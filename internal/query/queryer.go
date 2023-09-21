package query

// type Queryer interface {
// 	Query(string, QueryLevel) []int64
// 	Use(Ranker)
// 	SetIndexManager(IndexManager)
// }

// type Ranker interface {
// 	Rank(map[string][]Index) //
// }
import (
	"fts/internal/common"
	"fts/internal/types"
	"strings"
)

type QueryBuilder struct {
	Tokenizer types.Tokenizer
	imanager  types.IndexManager
	//ranker    types.Ranker
	field string
}

func NewQueryBuilder(tzr types.Tokenizer, field string) *QueryBuilder {
	return &QueryBuilder{
		Tokenizer: tzr,
		field:     field,
	}
}

func (eq *QueryBuilder) Query(text string, l types.QueryLevel, args ...any) ([]types.QueryReuslt, map[string]types.Pair, []int64, string, []string) {
	if eq.Tokenizer == nil {
		return nil, nil, nil, "", nil
	}

	switch l {
	case types.AT_LEAST:
		return eq.queryM(text, args[0].(bool), args[1].(bool), args[2:]...)
	case types.AT_OR:
		return eq.queryU(text)
	case types.AT_AND:
		return eq.queryA(text)
	default:
		return nil, nil, nil, "", nil
	}
}

func (eq *QueryBuilder) queryM(text string,
	expand bool,
	sign bool,
	args ...any,
) ([]types.QueryReuslt,
	map[string]types.Pair,
	[]int64,
	string,
	[]string,
) {

	var (
		maps  = make(map[string][]int64)    //token 和文档的映射关系
		infos = make(map[string]types.Pair) //文档出现的次数
		//indexes  = make(map[int64]types.Index) //索引表
		docs     = make([]int64, 0)
		loadmaps = make(map[string][]int64) //
		tokens   = make([]string, 0)
	)

	for _, v := range eq.Tokenizer.Analyze(text) {
		index := eq.imanager.GetIndex(v.Token(), eq.field)
		result := index.QueryAllDoc()
		tokens = append(tokens, v.Token())
		infos[v.Token()] = types.Pair{
			Maps: result.Info,
		}
		docs = common.GetUnionSet(docs, result.Ids)
		if _, ok := maps[v.Token()]; !ok {
			maps[v.Token()] = make([]int64, 0)
		}
		maps[v.Token()] = append(maps[v.Token()], result.Ids...)
	}

	var si []interface{}
	rate := args[0].(float64)

	k := int(float64(len(maps)) * rate)
	if k < 1 {
		k = 1
	}
	if k < len(maps) {
		for kz := range maps {
			si = append(si, kz)
		}
	}

	// get all subset
	var sis [][]interface{}
	if expand {
		sis = common.AtLeaseKSubSet(si, k)
	} else {
		sis = common.KSubSet(si, k)
	}

	// merge all ids
	for _, v := range sis {
		subset := []string{}
		ids := []int64{}

		if sign {
			// "|"
			for _, vv := range v {
				subset = append(subset, vv.(string))             // 字符串组合
				ids = common.GetUnionSet(ids, maps[vv.(string)]) //两者的并集
			}
			loadmaps[strings.Join(subset, "|")] = ids // eg maps["washinton|beijing"]
		} else {
			// "&"
			for _, vv := range v {
				subset = append(subset, vv.(string))              // 字符串组合
				ids = common.CommonSubset(ids, maps[vv.(string)]) //两者的并集
			}
			loadmaps[strings.Join(subset, "|")] = ids // eg maps["washinton&beijing"]
		}
	}
	// rank
	//seq := eq.ranker.Rank(loadmaps, infos)

	result := []types.QueryReuslt{}

	for k, v := range loadmaps {
		result = append(result, types.QueryReuslt{
			Docs:   v,
			Tokens: k,
		})
	}

	tag := "|"
	if !sign {
		tag = "&"
	}
	return result, infos, docs, tag, tokens
}

// all "|"
func (eq *QueryBuilder) queryU(text string) ([]types.QueryReuslt, map[string]types.Pair, []int64, string, []string) {
	var (
		maps  = make(map[string][]int64)    //token 和文档的映射关系
		infos = make(map[string]types.Pair) //文档出现的次数
		docs  = make([]int64, 0)
		//indexes  = make(map[int64]types.Index) //索引表
		loadmaps = make(map[string][]int64) //
		tokens   = make([]string, 0)
	)

	for _, v := range eq.Tokenizer.Analyze(text) {
		index := eq.imanager.GetIndex(v.Token(), eq.field)
		result := index.QueryAllDoc()
		tokens = append(tokens, v.Token())
		infos[v.Token()] = types.Pair{
			Maps: result.Info,
		}
		docs = common.GetUnionSet(docs, result.Ids)
		if _, ok := maps[v.Token()]; !ok {
			maps[v.Token()] = make([]int64, 0)
		}
		maps[v.Token()] = append(maps[v.Token()], result.Ids...)
	}
	var id []int64
	var si []string
	for k, v := range maps {
		si = append(si, k)
		id = append(id, common.GetUnionSet(id, v)...)
	}

	loadmaps[strings.Join(si, "|")] = id

	// rank

	result := []types.QueryReuslt{}

	for k, v := range loadmaps {
		result = append(result, types.QueryReuslt{
			Docs:   v,
			Tokens: k,
		})
	}

	return result, infos, docs, "|", tokens
}

// all "&"
func (eq *QueryBuilder) queryA(text string) ([]types.QueryReuslt, map[string]types.Pair, []int64, string, []string) {
	var (
		maps  = make(map[string][]int64)    //token 和文档的映射关系
		infos = make(map[string]types.Pair) //文档出现的次数
		docs  = make([]int64, 0)
		//indexes  = make(map[int64]types.Index) //索引表
		loadmaps = make(map[string][]int64) //
		tokens   = make([]string, 0)
	)

	for _, v := range eq.Tokenizer.Analyze(text) {
		index := eq.imanager.GetIndex(v.Token(), eq.field)
		result := index.QueryAllDoc()
		tokens = append(tokens, v.Token())
		infos[v.Token()] = types.Pair{
			Maps: result.Info,
		}
		docs = common.GetUnionSet(docs, result.Ids)
		if _, ok := maps[v.Token()]; !ok {
			maps[v.Token()] = make([]int64, 0)
		}
		maps[v.Token()] = append(maps[v.Token()], result.Ids...)
	}
	var id []int64
	var si []string
	for k, v := range maps {
		si = append(si, k)
		id = append(id, common.GetUnionSet(id, v)...)
	}

	loadmaps[strings.Join(si, "&")] = id

	// rank

	result := []types.QueryReuslt{}

	for k, v := range loadmaps {
		result = append(result, types.QueryReuslt{
			Docs:   v,
			Tokens: k,
		})
	}

	return result, infos, docs, "|", tokens
}

func (eq *QueryBuilder) SetIndexManager(i types.IndexManager) {
	eq.imanager = i
}
