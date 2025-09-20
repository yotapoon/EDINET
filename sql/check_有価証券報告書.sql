
/*
SELECT *
FROM EDINET..Submission AS SUBMISSION
	LEFT JOIN EDINET..DocumentFormMaster AS DOC
		ON DOC.ordinance_code = SUBMISSION.ordinanceCode
			AND DOC.form_code = SUBMISSION.formCode
WHERE 1 = 1
	AND SUBMISSION.secCode = '72030'
	--AND DOC.doc_type LIKE N'有価証券報告書'
ORDER BY SUBMISSION.dateFile DESC

--delete from EDINET..Submission where dateFile = '2025-9-16'

select * from EDINET..Submission where dateFile = '2025-6-30' and formCode is null
SELECT * FROM EDINET..DocumentFormMaster
*/

select dateFile
		,seqNumber
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
	--AND csvFlag = 0 AND xbrlFlag = 1 -- 7件がヒット⇒とりあえず考慮しない
	--AND csvFlag = 0 AND pdfFlag = 1 -- 約6万件がヒット⇒数は多いがPDFが取れてもどうしようもないため考慮しない
	--AND docID = 'S100W9YI'
