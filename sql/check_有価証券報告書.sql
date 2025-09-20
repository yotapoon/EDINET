
/*
SELECT *
FROM EDINET..Submission AS SUBMISSION
	LEFT JOIN EDINET..DocumentFormMaster AS DOC
		ON DOC.ordinance_code = SUBMISSION.ordinanceCode
			AND DOC.form_code = SUBMISSION.formCode
WHERE 1 = 1
	AND SUBMISSION.secCode = '72030'
	--AND DOC.doc_type LIKE N'�L���،��񍐏�'
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
	--AND csvFlag = 0 AND xbrlFlag = 1 -- 7�����q�b�g�˂Ƃ肠�����l�����Ȃ�
	--AND csvFlag = 0 AND pdfFlag = 1 -- ��6�������q�b�g�ː��͑�����PDF�����Ă��ǂ����悤���Ȃ����ߍl�����Ȃ�
	--AND docID = 'S100W9YI'
