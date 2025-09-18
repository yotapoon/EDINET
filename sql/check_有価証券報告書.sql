

SELECT *
FROM EDINET..Submission AS SUBMISSION
	LEFT JOIN EDINET..DocumentFormMaster AS DOC
		ON DOC.ordinance_code = SUBMISSION.ordinanceCode
			AND DOC.form_code = SUBMISSION.formCode
WHERE 1 = 1
	AND SUBMISSION.secCode = '72030'
	--AND DOC.doc_type LIKE N'óLâøèÿåîïÒçêèë'
ORDER BY SUBMISSION.dateFile DESC

--delete from EDINET..Submission where dateFile = '2025-9-16'