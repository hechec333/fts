from snownlp import SnowNLP

class Result:
    def __init__(self,result,error) -> None:
        self.list = result
        self.error = error
        self.ok = False

def TagText(text):
    result = Result(list(),"")
    l = list()
    try:    
        s = SnowNLP(text)
        for item in s.listTags:
            l.append(item)
    except:
        result.list = list()
        result.error = "TagText errors"
        result.ok = False
    else:
        result.list = l
        result.ok = True
    finally:
        return result



res = TagText("近日，主播“秀才”账号显示违反平台相关规定，已被封禁。据每日人物报道，一份《检举税收违法行为受理回执》显示，2023年8月15日，有人实名向国家税务总局亳州市税务局稽查局，检举了徐xx(网名秀才）涉嫌税收违法行为，已于当天被受理。与“秀才”一样引发关注的还有“一笑倾城”，她的社交平台目前显示“家中有事休息”，最新一条动态更新于8月21日")

print(res.list)


res = TagText("近日，主播“秀才”账号显示违反平台相关规定，已被封禁。据每日人物报道，一份《检举税收违法行为受理回执》显示，2023年8月15日，有人实名向国家税务总局亳州市税务局稽查局，检举了徐xx(网名秀才）涉嫌税收违法行为，已于当天被受理。与“秀才”一样引发关注的还有“一笑倾城”，她的社交平台目前显示“家中有事休息”，最新一条动态更新于8月21日")

print(res.list)