
/*
SELECT *
FROM EDINET..Submission AS SUBMISSION
	LEFT JOIN EDINET..DocumentFormMaster AS DOC
		ON DOC.ordinance_code = SUBMISSION.ordinanceCode
			AND DOC.form_code = SUBMISSION.formCode
WHERE 1 = 1
	--AND SUBMISSION.secCode = '80580' -- 三菱商事
	AND DOC.doc_type LIKE N'%大量保有%'
ORDER BY SUBMISSION.dateFile DESC

--delete from EDINET..Submission where dateFile = '2025-9-16'

select * from EDINET..Submission where dateFile = '2025-6-30' and formCode is null
SELECT * FROM EDINET..DocumentFormMaster
SELECT * FROM EDINET..DocumentFormMaster where doctypecode = '120'
*/

select dateFile
		,seqNumber
		,SUBMISSION.filerName
		,docID
		,edinetCode
		,secCode
		,filerName
		,fundCode
		,ordinanceCode
		,doc.ordinance_name
		,DOC.form_name
		,formCode
		,SUBMISSION.docTypeCode
		--,DOC.doc_type
		,docDescription
		,issuerEdinetCode
		,subjectEdinetCode
		,parentDocID
		,xbrlFlag
		,pdfFlag
		,csvFlag
		,legalStatus
		,FlagLoadCsv
from edinet..Submission AS SUBMISSION
	LEFT JOIN EDINET..DocumentFormMaster AS DOC
		ON DOC.ordinance_code = SUBMISSION.ordinanceCode
			AND DOC.form_code = SUBMISSION.formCode
where 1 = 1
	AND SUBMISSION.filerName LIKE N'%トヨタ自動車%' AND SUBMISSION.formCode = '030000' -- トヨタの有報
	--AND SUBMISSION.filerName LIKE N'%MS&AD%' AND SUBMISSION.formCode = '030000' -- MS&ADの有報
	--AND csvFlag = 0 AND xbrlFlag = 1 -- 7件がヒット⇒とりあえず考慮しない
	--AND csvFlag = 0 AND pdfFlag = 1 -- 約6万件がヒット⇒数は多いがPDFが取れてもどうしようもないため考慮しない
	--AND docID = 'S100W9YI'
ORDER BY dateFile DESC

