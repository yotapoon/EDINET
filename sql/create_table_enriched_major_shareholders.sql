-- 名寄せ後の大株主情報テーブル
CREATE TABLE EnrichedMajorShareholders (
    -- 元テーブルのキー情報
    SubmissionDate DATE,
    SecuritiesCode NVARCHAR(5),
    shareholderId INT,
    
    -- 元テーブルのデータ
    MajorShareholderName NVARCHAR(255),
    VotingRightsRatio FLOAT,
    NumberOfSharesHeld FLOAT,

    -- 名寄せによって追加された情報
    matchedEdinetCode NVARCHAR(6),
    matchedSecCode NVARCHAR(5),
    matchMethod NVARCHAR(50), -- (例: exact, fuzzy, manual)

    -- 主キー
    PRIMARY KEY (SubmissionDate, SecuritiesCode, shareholderId)
);
