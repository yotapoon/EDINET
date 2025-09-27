
/*
SELECT *
FROM EDINET..Submission AS SUBMISSION
	LEFT JOIN EDINET..DocumentFormMaster AS DOC
		ON DOC.ordinance_code = SUBMISSION.ordinanceCode
			AND DOC.form_code = SUBMISSION.formCode
WHERE 1 = 1
	--AND SUBMISSION.secCode = '80580' -- �O�H����
	AND DOC.doc_type LIKE N'%��ʕۗL%'
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
	AND SUBMISSION.filerName LIKE N'%�g���^������%' AND SUBMISSION.formCode = '030000' -- �g���^�̗L��
	--AND SUBMISSION.filerName LIKE N'%MS&AD%' AND SUBMISSION.formCode = '030000' -- MS&AD�̗L��
	--AND csvFlag = 0 AND xbrlFlag = 1 -- 7�����q�b�g�˂Ƃ肠�����l�����Ȃ�
	--AND csvFlag = 0 AND pdfFlag = 1 -- ��6�������q�b�g�ː��͑�����PDF�����Ă��ǂ����悤���Ȃ����ߍl�����Ȃ�
	--AND docID = 'S100W9YI'
ORDER BY dateFile DESC

