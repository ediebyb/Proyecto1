import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Simula dataset grande (en real, carga un CSV de Kaggle; aquí generamos uno)
data = pd.DataFrame({
    'Producto': np.random.choice(['A', 'B', 'C'], 1500),
    'Ventas': np.random.randint(50, 300, 1500).astype(float),
    'Fecha': pd.date_range(start='2023-01-01', periods=1500, freq='D')
})
data.loc[::10, 'Ventas'] = np.nan  # Introduce nulos para limpieza

# Procesamiento distribuido simulado: En chunks para eficiencia
chunks = [data[i:i+500] for i in range(0, len(data), 500)]
processed = []
for chunk in chunks:
    chunk['Ventas'] = chunk['Ventas'].fillna(chunk['Ventas'].mean())
    processed.append(chunk)
df = pd.concat(processed)

# Análisis avanzado: Ventas mensuales por producto
df['Mes'] = df['Fecha'].dt.to_period('M')
ventas_mensuales = df.groupby(['Mes', 'Producto'])['Ventas'].sum().unstack()

# Visualización: Heatmap interactivo
sns.heatmap(ventas_mensuales, annot=True, cmap='YlGnBu')
plt.title('Ventas Mensuales por Producto')
plt.show()

print("Reflexión: Integré procesamiento en chunks para simular entornos distribuidos como Spark (Módulo 07), combinado con limpieza y análisis en pandas (Módulos 02-03). Esto mejora la eficiencia en datasets reales y refuerza habilidades para big data.")