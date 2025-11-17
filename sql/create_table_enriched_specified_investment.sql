-- 名寄せ後の特定投資株式情報テーブル
CREATE TABLE EnrichedSpecifiedInvestment (
    -- 元テーブルのキー情報
    SubmissionDate DATE,
    SecuritiesCode NVARCHAR(5),
    HoldingEntity NVARCHAR(255),
    rowId INT,

    -- 元テーブルのデータ
    NameOfSecurities NVARCHAR(255),
    NumberOfSharesHeldCurrentYear FLOAT,
    BookValueCurrentYear FLOAT,
    NumberOfSharesHeldPriorYear FLOAT,
    BookValuePriorYear FLOAT,
    HoldingPurpose NVARCHAR(MAX),
    CrossShareholdingStatus NVARCHAR(MAX),

    -- 名寄せによって追加された情報
    matchedEdinetCode NVARCHAR(6),
    matchedSecCode NVARCHAR(5),
    matchMethod NVARCHAR(50),

    -- 主キー
    PRIMARY KEY (SubmissionDate, SecuritiesCode, HoldingEntity, rowId)
);

--select * from EDINET..EnrichedSpecifiedInvestment