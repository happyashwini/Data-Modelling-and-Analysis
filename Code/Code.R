library(RSQLite)
setwd("D:/Master/Data Science")

conn <- dbConnect(SQLite(),"Coursework.db")

question3 <- dbSendQuery(conn, "Select DISTINCT LocalAuthorityName,WardName,  substr(qa.QuarterCode, -2,2) Quarter,
AveragePrice from Ward w 
inner join LocalAuthority la on la.LocalAuthorityCode=w.LocalAuthorityCodeId
inner join WardQuarter wq on wq.WardCodeId = w. WardCode
inner join ( select q.QuarterCode , (q.Price + qq.Price)/2 AveragePrice from Quarter qq 
inner join Quarter q on substr(qq.QuarterCode, -4,4)= substr(q.QuarterCode, -4,4)where q.Year <> qq.Year) qa on 
wq.QuarterCodeId = qa. QuarterCode")

dbFetch(question3)

question4 <- dbSendQuery(conn, "Select distinct la.LocalAuthorityName,wa.WardName,c.percentage
from LocalAuthority la 
inner JOIN ward wa
on la.LocalAuthorityCode =wa.LocalAuthorityCodeId
inner JOIN WardQuarter waq
on 
wa.WardCode = waq.WardCodeId
inner join
(
select substr(qa.quartercode,-4,3) as quartercode,qa.year,qb.year,sum(qa.price),sum(qb.price), (qa.price-qb.price), 
(((qa.price-qb.price)*100)/(qa.price))||'%'  as percentage
 from quarter qa inner join
	 quarter qb on substr(qa.quartercode, -4,3) = substr(qb.quartercode,-4,3)
	 
	 where qa.year = '2020' and qb.year = '2021'
 group by substr(qa.quartercode,-4,3), qa.year,qb.year
 )c
 on 
substr(waq.QuarterCodeId,-4,3)=c.quartercode
order by 1,2,3")

dbFetch(question4)

question5 <- dbSendQuery(conn, "Select c.LocalAuthorityName,c.WardName,c.QuarterCodeId,c.year,c.month,c.price from (
select distinct la.LocalAuthorityName,wa.WardName,substr(waq.QuarterCodeId,-2,2) QuarterCodeId,qb.year,qb.month,qb.price,
dense_rank() over (PARTITION by la.LocalAuthorityName,substr(waq.QuarterCodeId,-2,2)
 order by price desc ) rn 
from LocalAuthority la 
inner JOIN ward wa
on la.LocalAuthorityCode =wa.LocalAuthorityCodeId
inner JOIN WardQuarter waq
on 
wa.WardCode = waq.WardCodeId
inner join
quarter qb
on
substr(waq.QuarterCodeId,-4,4)=substr(qb.QuarterCode,-4,4)) c
where rn=1
order by 1,2,3")

dbFetch(question5)

question6 <- dbSendQuery(conn, "Select c.ConstituencyName,wa.WardNames,b.AverageSpeed,b.SuperfastAvalability
from Constituency c
left join 
WardBroadband w
on c.ConstituencyCode=w.ConstituencyCodeId
left join Wards wa
on w.WardCodeIds=wa.WardCodes
left join Broadbands b
on  wa.WardCodes=b.BroadbandCode
order by 1,2")

dbFetch(question6)

question7 <- dbSendQuery(conn, "Select n.RegionName,c.ConstituencyName,wa.WardNames,b.AverageSpeed,b.SuperfastAvalability,
b.ReceivingUnder10Mbps,b.ReceivingOver30Mbps
from Constituency c, NationalRegion n
left join 
WardBroadband w
on c.ConstituencyCode=w.ConstituencyCodeId
left join Wards wa
on w.WardCodeIds=wa.WardCodes
left join Broadbands b
on  wa.WardCodes=b.BroadbandCode
order by 1,2")

dbFetch(question7)

question8 <- dbSendQuery(conn, "Select aaa.AreaName, ag.AID, ag.BID, ag.CID, ag. AveragePrice from (
Select ap.AreaCodeId, AID, BID, BandCodeId CID, round((abc.Price+ap.Price)/2,2) AveragePrice from 
(select a.AreaCodeId , a.BandCodeId AID , ab.BandCodeId BID, (a.Price+ AB.Price)/2 Price
from AreaBand a inner join AreaBand ab on a.AreaCodeId=ab.AreaCodeId 
where a.BandCodeId<>ab.BandCodeId) ap inner join AreaBand abc on abc.AreaCodeId= ap.AreaCodeId
where abc.BandCodeId <> ap.AID and abc.BandCodeId<>ap.BID ) ag inner join Area aaa on ag.AreaCodeId = aaa.AreaCode 
where ag.AID ='A' and ag.BID='B' and ag.CID='C'")

dbFetch(question8)

question9 <- dbSendQuery(conn, "Select CounsilName, AreaCodeId, AreaName,BandCodeId, Price, 
round((Lead(Price,-1) OVER
(Order by AreaCodeId) - Price),2) as Difference
From AreaBand B, Area A, Counsil C
where AreaCodeId in('A1','A2') and BandCodeId='A'
and B.AreaCodeId=A.AreaCode and C.CounsilCode =A.CounsilCodeId")

dbFetch(question9)










































































