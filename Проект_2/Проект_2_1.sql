-- Очистка от дублей с помощью ctid - атрибут, который указывает физическое расположение версии строки в её таблице
DELETE FROM dm.client
WHERE ctid NOT IN (
	SELECT MIN(ctid)
	FROM dm.client
	GROUP BY client_rk, effective_from_date
);
