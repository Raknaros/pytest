import numpy as np


def trazar_perceptron(datos, indice_inicial):
    """
    Ejecuta el algoritmo Perceptrón y devuelve el número de errores y la progresión de theta.

    Args:
        datos (list): Una lista de tuplas, donde cada tupla es (vector_x, etiqueta_y).
        indice_inicial (int): El índice del punto de datos con el que empezar (0 para x_1, 1 para x_2, etc.).
    """
    # Inicialización
    theta = np.array([0.0, 0.0])
    progresion_theta = []
    errores_totales = 0

    indices = list(range(len(datos)))

    # Bucle principal hasta la convergencia
    convergido = False
    while not convergido:
        errores_en_ciclo = 0

        # Iniciar el ciclo desde el punto correcto
        for i in range(len(datos)):
            # Obtenemos el índice del punto a revisar en este ciclo
            indice_actual = (indice_inicial + i) % len(datos)
            x, y = datos[indice_actual]
            x_vec = np.array(x)

            # 1. Comprobar si hay un error
            if y * np.dot(theta, x_vec) <= 0:
                errores_totales += 1
                errores_en_ciclo += 1

                # 2. Actualizar theta si hay error
                theta = theta + y * x_vec
                progresion_theta.append(theta.tolist())

        # 3. Comprobar si ha convergido
        if errores_en_ciclo == 0:
            convergido = True

    return errores_totales, progresion_theta