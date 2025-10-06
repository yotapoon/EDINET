--DROP TABLE IF EXISTS EDINET.dbo.DocumentMetadata;

CREATE TABLE EDINET.dbo.DocumentMetadata (
  dateFile DATE NOT NULL,
  seqNumber INT NULL,
  docID CHAR(8) NOT NULL,
  edinetCode CHAR(6) NULL,
  secCode CHAR(5) NULL,
  JCN CHAR(13) NULL,
  filerName NVARCHAR(128) NULL,
  fundCode CHAR(6) NULL,
  ordinanceCode CHAR(3) NULL,
  formCode CHAR(6) NULL,
  docTypeCode CHAR(3) NULL, -- 書類種別コード
  periodStart DATE NULL,
  periodEnd DATE NULL,
  submitDateTime DATETIME NULL,
  docDescription NVARCHAR(147) NULL,
  issuerEdinetCode CHAR(6) NULL,
  subjectEdinetCode CHAR(6) NULL,
  subsidiaryEdinetCode NVARCHAR(69) NULL,
  currentReportReason NVARCHAR(1000) NULL,
  parentDocID CHAR(8) NULL,
  opeDateTime DATETIME NULL,
  withdrawalStatus CHAR(1) NULL,
  docInfoEditStatus CHAR(1) NULL,
  disclosureStatus CHAR(1) NULL,
  xbrlFlag BIT NULL,
  pdfFlag BIT NULL,
  attachDocFlag BIT NULL,
  englishDocFlag BIT NULL,
  csvFlag BIT NULL,
  legalStatus CHAR(1) NULL,
  csvLoadFlag BIT NULL

);

--ALTER TABLE EDINET.dbo.DocumentMetadata ADD FlagLoadCsv BIT DEFAULT 0;
ALTER TABLE EDINET.dbo.DocumentMetadata ADD CONSTRAINT PK_DocumentMetadata PRIMARY KEY (dateFile, seqNumber, docID)


CREATE NONCLUSTERED INDEX IX_DocumentMetadata_docID
  ON Edinet.dbo.DocumentMetadata(docID)

-- 提出日に係る索引
CREATE NONCLUSTERED INDEX IX_DocumentMetadata_dateFile
  ON Edinet.dbo.DocumentMetadata(dateFile)
  --INCLUDE (seqNumber, docID);

-- EDINETコードに係る索引
CREATE NONCLUSTERED INDEX IX_DocumentMetadata_edinetCode
  ON Edinet.dbo.DocumentMetadata(edinetCode)
  --INCLUDE (submitDateTime, docID, periodStart, periodEnd);

-- 証券コードに係る索引
CREATE NONCLUSTERED INDEX IX_DocumentMetadata_secCode
  ON Edinet.dbo.DocumentMetadata(secCode)
  --INCLUDE (submitDateTime, docID, periodStart, periodEnd);

--SELECT TOP 100 * FROM EDINET.dbo.DocumentMetadata ORDER BY DATEFILE DESC
--SELECT * FROM EDINET.dbo.DocumentMetadata WHERE docID = 'S100WM4O'

--DELETE FROM Edinet.dbo.DocumentMetadata WHERE dateFile >= '2025-10-1'

/*
WITH DuplicatesCTE AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY docID -- Group rows by the same docID
            ORDER BY submitDateTime DESC, opeDateTime DESC -- Order by newest submission time within each group
        ) AS RowNum
    FROM
        EDINET.dbo.DocumentMetadata
)
DELETE FROM DuplicatesCTE
WHERE RowNum > 1; -- Delete all but the first (newest) record in each group
*/

/*
-- テーブル名を変更する
-- ステップ 1: テーブル名を 'Submission' から 'DocumentMetadata' に変更
EXEC sp_rename 'EDINET.dbo.Submission', 'DocumentMetadata';
GO

-- ステップ 2: カラム名をキャメルケースに統一
-- 'FlagLoadCsv' -> 'csvLoadFlag'
EXEC sp_rename 'EDINET.dbo.DocumentMetadata.FlagLoadCsv', 'csvLoadFlag', 'COLUMN';
GO

-- 確認用クエリ: 変更後のテーブル定義とデータを確認
SELECT TOP 10 * FROM EDINET.dbo.DocumentMetadata;
GO
*/