'''
Creado por: Alberto Gª Marin
Fecha creacion: 29/12/2023
'''

import mysql.connector as conector


class mysqlbd: 
    
    def __init__(self, nombre, datos_conexion):
        self.conn = None
        self.cursor = None
        self.nombre = nombre
        self.datos_conexion = datos_conexion
        self.host = datos_conexion["host"]
        self.puerto = datos_conexion["puerto"]
        self.usuario = datos_conexion["user"]
        self.password = datos_conexion["pass"]
        self.conectado = False
        self.Autocommit = False
        
    def conectar( self, modo = 'd', bbdd = '' ):
        '''
        Aqui podemos pasarle un parametro en la conexion buffered = True/False
        '''
        self.database = bbdd
        try:
            self.conn = conector.connect(
                host = self.host,
                user = self.usuario,
                password = self.password,
                database = self.database,
            )
            if self.conn != None:
                print("Conectado con exito !!!")
            self.conectado = True
            # self.cursor = self.conn.cursor(dictionary = True) # dictionary = True esto nos devuelve los registros como un diccionario
            self.cursor = self.conn.cursor(named_tuple = True) # named_tuple = True nos devuelve los registros como tuplas
            # self.cursor = self.conn.cursor(raw = True) # raw = True no hace la conversion a tipos python.
        except Exception as exc:
            print('Error en la conexión !!!')
            print(f'El error es: {exc}')    
            
    def desconectar(self, bbdd = '' ):
        try:
            self.conn.close()
            self.conectado = False
            self.cursor = None
            print('Desconectado!!!')
        except Exception as exc:
            print(f'Error en desconexión: El error es: {exc}')  
            
    def dameTodosRegistros(self, sql):
        try:
            self.cursor.execute(sql)
            resultado = self.cursor.fetchall()
        except Exception as exc:
            print(f'Error al ejecutar select: {exc}')
            resultado = None
        return resultado

    def dameUnRegistro(self, sql):
        resultado = self.dameTodosRegistros(sql)
        if len(resultado) > 1:
            return resultado[0]
        else:
            return resultado    
    
    def ejecutar(self, sql):
        try:
            self.cursor.execute(sql)   
        except Exception as exc:
            print(f'Error al ejecutar la operacion: {sql}.')
            
    def commit(self):
        try:
            self.conn.commit()
        except Exception as exc:
            print('No se ha podido ejecutar el Commit.')


    # Funciones para manejar el AutoCommit:
    # setAutocommit: para poner el AutoCommit a True
    # clearAutoCommit: para poner el AutoCommit a False ( por defecto )
    
    def setAutocommit(self):
        try:
            self.conn.autocommit = True
            self.Autocommit = True
            
        except Exception as exc:
            print('No se ha podido modificar el AutoCommit.')
            
    def clearAutocommit(self):
        try:
            self.conn.autocommit = False
            self.Autocommit = False
            
        except Exception as exc:
            print('No se ha podido modificar el AutoCommit')





if __name__ == '__main__':
    astrobd = {'host': '10.159.94.253', 'puerto': '3306', 'user': 'mysql', 'pass': 'omega'}
    miBD = mysqlbd('astrobd', astrobd)

    miBD.conectar('astro_platform')
    sql = "SELECT * FROM astro_platform.TRABAJOS LIMIT 2"
    resultados = miBD.dameTodosRegistros(sql)
    # resultados = miBD.dameUnRegistro(sql)
    
    print(resultados)
    if miBD.conectado:
        miBD.desconectar()

        