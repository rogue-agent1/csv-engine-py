class CSVEngine:
    def __init__(s): s.tables={}
    def load(s, name, text, sep=","):
        lines=[l.strip() for l in text.strip().split("\n") if l.strip()]
        headers=lines[0].split(sep)
        rows=[]
        for line in lines[1:]:
            vals=line.split(sep)
            row={}
            for i,h in enumerate(headers):
                v=vals[i].strip() if i<len(vals) else ""
                try: v=int(v)
                except:
                    try: v=float(v)
                    except: pass
                row[h.strip()]=v
            rows.append(row)
        s.tables[name]=rows
        return len(rows)
    def select(s, table, columns=None, where=None, order_by=None, limit=None):
        rows=s.tables.get(table,[])
        if where: rows=[r for r in rows if where(r)]
        if order_by: rows=sorted(rows,key=lambda r:r.get(order_by,0))
        if limit: rows=rows[:limit]
        if columns: rows=[{k:r[k] for k in columns if k in r} for r in rows]
        return rows
    def aggregate(s, table, group_by, agg_col, fn="sum"):
        groups={}
        for r in s.tables.get(table,[]):
            key=r.get(group_by);groups.setdefault(key,[]).append(r.get(agg_col,0))
        ops={"sum":sum,"avg":lambda x:sum(x)/len(x),"count":len,"min":min,"max":max}
        return{k:ops[fn](v) for k,v in groups.items()}
def demo():
    db=CSVEngine()
    data="product,qty,price\nApple,10,1.5\nBanana,20,0.75\nApple,5,1.5\nCherry,15,2.0\nBanana,8,0.75"
    db.load("sales", data)
    print("All:", db.select("sales"))
    print("Apples:", db.select("sales", where=lambda r: r["product"]=="Apple"))
    print("Sum qty:", db.aggregate("sales", "product", "qty", "sum"))
    print("Avg price:", db.aggregate("sales", "product", "price", "avg"))
if __name__ == "__main__": demo()
