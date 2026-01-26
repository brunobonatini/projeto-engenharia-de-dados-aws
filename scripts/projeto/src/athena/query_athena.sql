# Query de validação (Athena) - Total de clientes e média de idade por estado
spark.sql("""
SELECT
    estado,
    COUNT(*) AS total_clientes,
    ROUND(AVG(idade),1) AS idade_media
FROM (
    SELECT
        CASE
            WHEN estado = 'SPP' THEN 'SP'
            ELSE estado
        END AS estado,
        idade
    FROM clientes.clientes_analytics
)
GROUP BY estado
ORDER BY total_clientes DESC
""").show()