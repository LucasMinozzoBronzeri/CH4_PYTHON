import json
import oracledb


try:
     conn = oracledb.connect(user="RM552648",
                             password="160205",
                             host="oracle.fiap.com.br",
                             port=1521,
                             service_name="ORCL")
     cursor = conn.cursor()
except Exception as erro:
    print(f"Erro: {erro}")
    conexao = False
    print("conexão falhada")
else:
     conexao = True
     print("conexao feita")
     print(f"Versão do banco de dados Oracle: {conn.version}")

# Lendo o arquivo Json e armazenando os dados em uma lista de dicionários
def read_json(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print('Erro ao abrir o arquivo')
        return []

data = read_json('salesforce-dataframe.json')

# Cria um Json com dados filtrados ou adiciona dados ao Json principal em que o insert foi feito diretamente no Oracle
def inserir_dados(rows,nomejson):
    try:
        data = []
        column_names = [desc[0] for desc in cursor.description]
        for row in rows:
            row_dict = dict(zip(column_names, row))
            data.append(row_dict)
        with open(nomejson, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("Arquivo JSON criado com sucesso.")

    except IOError as ioe:
        print(f"Erro ao abrir o arquivo: {ioe}")

    except TypeError as te:
        print(f"Tipo de erro encontrado: {te}")

    except Exception as error:
        print(f"Erro: {error}")

# Formatação dos dados do select para ser mostrado no menu
def mostrar_dados(rows):
    try:
        print("-" * 200)
        for row in rows:
            output_line = ""
            for i, value in enumerate(row):
                output_line += f"{cursor.description[i][0]}: {value}"
                if i < len(row) - 1:
                    output_line += "\n"
            print(output_line)
            print("-" * 200)
    except IndexError as ie:
        print(f"Erro ao acessar o índice do cursor: {ie}")
    except TypeError as te:
        print(f"Erro de tipo ao processar os dados: {te}")
    except Exception as error:
        print(f"Erro: {error}")

# Menus utilizados na aplicação
def display_menu():
    print("1 - Listar todos os registros")
    print("2 - Adicionar novo registro")
    print("3 - Atualizar registro")
    print("4 - Deletar registro")
    print("0 - Sair")

def display_aceitarpolitica():
    print("Deseja filtrar por 'Aceitar politica'?")
    print("1 - Sim")
    print("2 - Não")

def display_aeitarpolitica_s_ou_n():
    print("Exibir dados em que 'Aceitar politica seja: '")
    print("1 - 'S'")
    print("2 - 'N'")

def display_menujson():
    print("Guardar dados em arquivo Json?")
    print("1 - Sim")
    print("2 - Não")

# Mostrando os dados do Json, dependendo das escolhas do usuario, o Json a ser criado pelo inserir_dados() vai ter o nome diferente, referente as escolhas
def read():
    try:
        print("Deseja escolher um país em especifico:")
        print("1 - Sim")
        print("2 - Não")
        opcaopais = input()

        if opcaopais == "1":
            pais = input("Digite o nome do país: ")
            display_aceitarpolitica()
            opcaopolitica = input()

            if opcaopolitica == "1":
                display_aeitarpolitica_s_ou_n()
                opcaosn = input()

                if opcaosn == "1":
                    cursor.execute(f"SELECT * FROM FORMULARIO_CLIENTE WHERE PAIS = '{pais}' AND ACEITAR_POLITICA = 'S' ORDER BY ID_CLIENTE")
                    rows = cursor.fetchall()
                    nomejson = f"clientes_{pais}_aceitar_politica_S.json"
                    mostrar_dados(rows)

                    display_menujson()
                    opcaojson = (input())

                    if opcaojson == "1":
                        inserir_dados(rows, nomejson)

                    elif opcaojson == "2":
                        print("Voltando ao menu...")


                elif opcaosn == "2":
                    cursor.execute(f"SELECT * FROM FORMULARIO_CLIENTE WHERE PAIS = '{pais}' AND ACEITAR_POLITICA = 'N' ORDER BY ID_CLIENTE")
                    rows = cursor.fetchall()
                    nomejson = f"clientes_{pais}_aceitar_politica_N.json"
                    mostrar_dados(rows)

                    display_menujson()
                    opcaojson = (input())

                    if opcaojson == "1":
                        inserir_dados(rows, nomejson)

                    elif opcaojson == "2":
                        print("Voltando ao menu...")

            elif opcaopolitica == "2":
                cursor.execute(f"SELECT * FROM FORMULARIO_CLIENTE WHERE PAIS = '{pais}' ORDER BY ID_CLIENTE")
                rows = cursor.fetchall()
                nomejson = f"clientes_{pais}.json"
                mostrar_dados(rows)

                display_menujson()
                opcaojson = (input())

                if opcaojson == "1":
                    inserir_dados(rows, nomejson)

                elif opcaojson == "2":
                    print("Voltando ao menu...")

        elif opcaopais == "2":
            display_aceitarpolitica()
            opcaopolitica = input()

            if opcaopolitica == "1":
                display_aeitarpolitica_s_ou_n()
                opcaosn = input()

                if opcaosn == "1":
                    cursor.execute(f"SELECT * FROM FORMULARIO_CLIENTE WHERE ACEITAR_POLITICA = 'S' ORDER BY ID_CLIENTE")
                    rows = cursor.fetchall()
                    nomejson = "clientes_aceitar_politica_S.json"
                    mostrar_dados(rows)

                    display_menujson()
                    opcaojson = (input())

                    if opcaojson == "1":
                        inserir_dados(rows, nomejson)

                    elif opcaojson == "2":
                        print("Voltando ao menu...")

                elif opcaosn == "2":
                    cursor.execute(f"SELECT * FROM FORMULARIO_CLIENTE WHERE ACEITAR_POLITICA = 'N' ORDER BY ID_CLIENTE")
                    rows = cursor.fetchall()
                    nomejson = "clientes_aceitar_politica_N.json"
                    mostrar_dados(rows)

                    display_menujson()
                    opcaojson = (input())

                    if opcaojson == "1":
                        inserir_dados(rows, nomejson)

                    elif opcaojson == "2":
                        print("Voltando ao menu...")

            elif opcaopolitica == "2":
                cursor.execute("SELECT * FROM FORMULARIO_CLIENTE ORDER BY ID_CLIENTE")
                rows = cursor.fetchall()
                mostrar_dados(rows)
                nomejson = "salesforce-dataframe.json"
                inserir_dados(rows, nomejson)
    except Exception as erro:
        print(f"Erro: {erro}")


# Criando um novo registro e adicionando ao Json
def create(data):
    try:
        new_record = {}
        if not data:
            last_id = 0
        else:
            last_id = max(record['ID_CLIENTE'] for record in data) if data else 0

        next_id = last_id + 1
        cont = 0
        for key in data[0].keys():
            if cont == 0:
                new_record[key] = next_id
                cont += 1
            elif cont!= 0 and cont!= 4 and cont!= 6:
                while True:
                    user_input = input(f"Digite o valor para {key}: ")
                    if user_input.strip():
                        new_record[key] = user_input
                        cont += 1
                        break
                    else:
                        print("Entrada inválida Por favor, tente novamente.")
            elif cont == 4:
                while True:
                    user_input = input(f"Digite o valor para {key}(Digite apenas numeros): ")
                    if user_input.strip():
                        new_record[key] = user_input
                        cont += 1
                        break
                    else:
                        print("Entrada inválida Por favor, tente novamente.")
            else:
                while True:
                    user_input = input(f"Digite o valor para {key}(S ou N): ").upper()
                    if user_input in ['S', 'N']:
                        new_record[key] = user_input
                        break
                    else:
                        print("Entrada inválida Por favor, tente novamente.")

        data.append(new_record)

        sql_insert = "INSERT INTO FORMULARIO_CLIENTE (ID_CLIENTE, NOME, NOME_EMPRESA, EMAIL_CORPORATIVO, TELEFONE, PAIS, ACEITAR_POLITICA) VALUES (:1, :2, :3, :4, :5, :6, :7)"
        cursor.execute(sql_insert,
                               (new_record['ID_CLIENTE'], new_record['NOME'], new_record['NOME_EMPRESA'], new_record['EMAIL_CORPORATIVO'], new_record['TELEFONE'], new_record['PAIS'], new_record['ACEITAR_POLITICA']))

        conn.commit()

        with open('salesforce-dataframe.json', 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    except ValueError:
        print("Valor inválido!!!")
    except Exception as error:
        print(f"Erro: {error}")


# Atualizando um registro específico no Json, caso o usuario queira cancelar o update, é só digitar 0
def update(id_cliente):
    try:
        cont = 0
        cursor.execute(f"SELECT * FROM FORMULARIO_CLIENTE WHERE ID_CLIENTE = {id_cliente}")
        row = cursor.fetchone()
        if row is None:
            print("Registro não encontrado.")
            return

        column_names = [desc[0] for desc in cursor.description]

        row_dict = dict(zip(column_names, row))

        for key in row_dict:
            if key != 'ID_CLIENTE':
                print("Digite 0 para cancelar")
                if cont != 3 and cont != 5:
                    new_value = input(f"Digite o novo valor para {key} (deixe em branco para manter o atual: {row_dict[key]}): ")
                    cont += 1

                elif cont == 3:
                    new_value = input(f"Digite o novo valor para {key} (apenas numeros ex: 123456789) (deixe em branco para manter o atual: {row_dict[key]}): ")
                    cont += 1

                elif cont == 5:
                    new_value = input(f"Digite o novo valor para {key} (S ou N) (deixe em branco para manter o atual: {row_dict[key]}): ").upper()
                    cont += 1

                if new_value == "0":
                    break
                elif new_value:
                    row_dict[key] = new_value

        if new_value == "0":
            print("Cancelando...")

        elif new_value != "0":

            update_fields = ', '.join([f"{k} = :{k}" for k in row_dict if k != 'ID_CLIENTE'])
            update_sql = f"UPDATE FORMULARIO_CLIENTE SET {update_fields} WHERE ID_CLIENTE = :ID_CLIENTE"
            cursor.execute(update_sql, row_dict)
            conn.commit()
            print("Registro atualizado com sucesso.")

            with open("salesforce-dataframe.json", 'r', encoding='utf-8') as file:
                data = json.load(file)

            found = False
            for entry in data:
                if entry['ID_CLIENTE'] == id_cliente:
                    entry.update(row_dict)
                    found = True
                    break

            if not found:
                print("ID_CLIENTE não encontrado no JSON.")
                return

            with open("salesforce-dataframe.json", 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)
            print("Arquivo JSON atualizado com sucesso.")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")

# Deleta o registro de acordo com o ID selecionado
def delete(data):
    try:
        cursor.execute("SELECT * FROM FORMULARIO_CLIENTE ORDER BY ID_CLIENTE")
        rows = cursor.fetchall()
        mostrar_dados(rows)
        id_to_delete = int(input("Digite o ID do registro a ser deletado: "))
        delete_sql = "DELETE FROM FORMULARIO_CLIENTE WHERE ID_CLIENTE = :1"
        cursor.execute(delete_sql, (id_to_delete,))
        conn.commit()

        for index, record in enumerate(data):
            if record['ID_CLIENTE'] == id_to_delete:
                break

        if index < len(data):
            del data[index]
            with open('salesforce-dataframe.json', 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)
            print("Registro deletado com sucesso do arquivo JSON.")
        else:
            print("Não foi possível encontrar o registro no arquivo JSON para exclusão.")
    except ValueError:
        print('Valor inválido')
    except Exception as error:
        print(f"Erro: {error}")


# Interpreta a escolha do usuário e puxa a função a ser utilizada
def main():
    try:
        while True:
            display_menu()
            opcao = input("Escolha uma opção: ")
            if opcao == "1":
                read()
            elif opcao == "2":
                create(data)
            elif opcao == "3":
                cursor.execute("SELECT * FROM FORMULARIO_CLIENTE ORDER BY ID_CLIENTE")
                rows = cursor.fetchall()
                mostrar_dados(rows)
                id_cliente = int(input("Digite o ID do cliente que deseja atualizar: "))
                update(id_cliente)
            elif opcao == "4":
                delete(data)
            elif opcao == "0":
                print('Encerrando...')
                break
            else:
                print("Opção inválida. Tente novamente.")
    except Exception as erro:
        print(f"Erro: {erro}")


if __name__ == "__main__":
    main()
