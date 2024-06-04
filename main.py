from fastapi import FastAPI, HTTPException, Body, status
from pymongo import MongoClient
import httpx
import asyncio
from collections import defaultdict
from datetime import datetime
import calendar

app = FastAPI()

# Conexão com o banco de dados MongoDB
client = MongoClient(
    "mongodb+srv://chatbotTake:blipAutomation@cluster0.qm1oi6z.mongodb.net/")
db = client["earnings-Take"]
collection = db["chatbot-earnings"]

# Rota para adicionar chatbot e key
@app.post("/add-chatbot", status_code=status.HTTP_201_CREATED)
async def add_chatbot(chatbot: str = Body(...), key: str = Body(...)):
    # Verificar se o chatbot já existe no banco de dados
    if collection.find_one({"chatbot": chatbot}):
        raise HTTPException(
            status_code=400, detail="Chatbot já existe no banco de dados.")

    # Inserir chatbot e key no banco de dados
    result = collection.insert_one({"chatbot": chatbot, "key": key})
    return {"chatbot": chatbot, "key": key}

# Rota para buscar chatbot e key
@app.post("/search-chatbot")
async def search_chatbot(chatbot_data: dict = Body(...)):
    # Buscar chatbot no banco de dados
    chatbot = chatbot_data.get("chatbot")
    if not chatbot:
        raise HTTPException(
            status_code=422, detail="Campo 'chatbot' é obrigatório.")

    chatbot_data = collection.find_one({"chatbot": chatbot})
    if not chatbot_data:
        raise HTTPException(
            status_code=404, detail="Chatbot não encontrado no banco de dados.")

    return {"chatbot": chatbot_data["chatbot"], "key": chatbot_data["key"]}

# Rota para editar chatbot
@app.put("/edit-chatbot")
async def edit_chatbot(chatbot: str = Body(...), key: str = Body(...)):
    # Verificar se o chatbot existe no banco de dados
    if not collection.find_one({"chatbot": chatbot}):
        raise HTTPException(
            status_code=404, detail="Chatbot não encontrado no banco de dados.")

    # Atualizar a key do chatbot no banco de dados
    collection.update_one({"chatbot": chatbot}, {"$set": {"key": key}})
    return {"message": "Chatbot atualizado com sucesso.", "chatbot": chatbot, "key": key}

# Rota para excluir chatbot
@app.delete("/delete-chatbot")
async def delete_chatbot(chatbot: str):
    # Verificar se o chatbot existe no banco de dados
    if not collection.find_one({"chatbot": chatbot}):
        raise HTTPException(
            status_code=404, detail="Chatbot não encontrado no banco de dados.")

    # Excluir o chatbot do banco de dados
    collection.delete_one({"chatbot": chatbot})
    return {"message": "Chatbot excluído com sucesso.", "chatbot": chatbot}


def get_chatbots_from_db():
    # Consultar o banco de dados para obter todos os chatbots com suas chaves
    chatbots_data = collection.find({}, {"_id": 0, "chatbot": 1, "key": 1})
    chatbots = [{"chatbot": chatbot["chatbot"], "key": chatbot["key"]}
                for chatbot in chatbots_data]
    return chatbots

# Rota para retornar todos os chatbots com suas chaves
@app.get("/get-chatbots")
async def get_chatbots():
    # Consultar o banco de dados para obter todos os chatbots com suas chaves
    chatbots_data = collection.find({}, {"_id": 0, "chatbot": 1, "key": 1})
    chatbots = [{"chatbot": chatbot["chatbot"], "key": chatbot["key"]}
                for chatbot in chatbots_data]
    return {"chatbots": chatbots}

# Função para realizar a requisição POST para a URL específica com a chave de autorização
async def make_request(url, headers, data):
    try:
        async with httpx.AsyncClient(timeout=100.0) as client:
            response = await client.post(url, headers=headers, json=data)
        return response
    except httpx.ConnectError as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro de conexão: Blip {exc}")
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro desconhecido: {exc}")


def date_difference_in_days(start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    difference = (end_date - start_date).days
    return difference + 1  # Adicionamos 1 para incluir o dia final no cálculo


# Rota para o helper-events
@app.post("/helper-events")
async def helper_events(event_data: dict = Body(...)):
    try:
        month = event_data.get("month")
        if not month:
            raise HTTPException(
                status_code=400, detail="Campo 'month' é obrigatório.")

        # Obter todas as chaves de chatbots
        chatbots = get_chatbots_from_db()
        if not chatbots:
            raise HTTPException(
                status_code=404, detail="Nenhum chatbot encontrado no banco de dados.")

        tasks = []

        # Calcular o ano e o número do mês
        year = datetime.now().year
        month_map = {
            "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
            "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
            "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
        }
        month_number = month_map.get(month.lower())
        if not month_number:
            raise HTTPException(status_code=400, detail="Mês inválido.")

        # Calcular as datas de início e fim do mês
        start_date = f"{year}-{month_number}-01"
        end_date = f"{year}-{month_number}-{calendar.monthrange(year, int(month_number))[1]}"

        # Construir a URI com as datas calculadas
        uri = f"/event-track/Metricas-Fluxo?startDate={start_date}&endDate={end_date}&$take=10000"

        max_retries = 3  # Defina o número máximo de tentativas
        for retry in range(max_retries):
            tasks = []  # Limpe as tarefas antes de cada tentativa
            print(f"Iniciando tentativa {retry + 1} de {max_retries}")

            for chatbot in chatbots:
                key = chatbot.get("key")
                if key:
                    headers = {"Authorization": key}
                    task = asyncio.create_task(make_request("https://intelbras.http.msging.net/commands", headers, {
                        "id": "444", "to": "postmaster@analytics.msging.net", "method": "get", "uri": uri}))
                    tasks.append(task)

            # Definir a lista responses antes do loop
            responses = await asyncio.gather(*tasks)

            results = {}
            total = defaultdict(int)

            for i, response in enumerate(responses):
                try:
                    response.raise_for_status()
                    data = response.json()
                    items = data["resource"]["items"]

                    total_retidos = 0
                    for item in items:
                        # Renomear a ação para "retidos" se for "retido no bot"
                        action = item["action"]
                        if action == "Retidos no bot":
                            action = "Retidos"

                        if action == "Retidos":
                            total_retidos += item["count"]

                        chatbot_name = chatbots[i]["chatbot"]
                        chatbots[i][action] = chatbots[i].get(action, 0) + item["count"]

                        if action == "Retidos":
                            total[action] += item["count"]

                except httpx.HTTPStatusError as e:
                    error_message = f"Erro na requisição para {chatbots[i]['chatbot']}: {e}"
                    print(error_message)
                    continue  # Continuar com a próxima iteração em caso de erro

                except Exception as e:
                    error_message = f"Erro desconhecido na tentativa {retry + 1} para {chatbots[i]['chatbot']}: {e}"
                    print(error_message)
                    continue  # Continuar com a próxima iteração em caso de erro desconhecido

            total_result = {
                "Retidos_bot": sum([item.get("Retidos", 0) for item in chatbots]),
                "start_date": start_date,
                "end_date": end_date
            }

            results.update({chatbot["chatbot"]: chatbot for chatbot in chatbots})
            results["total"] = total_result

            print(f"Tentativa {retry + 1} de {max_retries} concluída")

            return results

        raise HTTPException(
            status_code=500, detail="Falhou na requisição Take após várias tentativas")

    except Exception as exc:
        error_message = f"Erro de processamento events: Não foi possível processar os dados corretamente vindo da Take. Tente novamente: {exc}"
        print(error_message)
        raise HTTPException(
            status_code=500, detail=error_message)


# Rota para o helper-interaction
@app.post("/helper-interaction")
async def helper_interaction(event_data: dict = Body(...)):
    try:
        month = event_data.get("month")
        if not month:
            raise HTTPException(
                status_code=400, detail="Campo 'month' é obrigatório.")

        chatbots = get_chatbots_from_db()
        if not chatbots:
            raise HTTPException(
                status_code=404, detail="Nenhum chatbot encontrado no banco de dados.")

        tasks = []

        year = datetime.now().year
        month_map = {
            "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
            "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
            "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
        }
        month_number = month_map.get(month.lower())
        if not month_number:
            raise HTTPException(status_code=400, detail="Mês inválido.")

        start_date = f"{year}-{month_number}-01"
        end_date = f"{year}-{month_number}-{calendar.monthrange(year, int(month_number))[1]}"

        uri = f"/event-track/Interacoes-Fluxo?startDate={start_date}&endDate={end_date}&$take=10000"

        max_retries = 5  # Defina o número máximo de tentativas
        for retry in range(max_retries):
            tasks = []  # Limpe as tarefas antes de cada tentativa

            for chatbot in chatbots:
                key = chatbot.get("key")
                if key:
                    headers = {"Authorization": key}
                    task = asyncio.create_task(make_request("https://intelbras.http.msging.net/commands", headers, {
                        "id": "444", "to": "postmaster@analytics.msging.net", "method": "get", "uri": uri}))
                    tasks.append(task)

            # Definir a lista responses antes do loop
            responses = await asyncio.gather(*tasks)

            total_interacoes_geral = 0  # Inicializando como 0

            for i, response in enumerate(responses):
                try:
                    response.raise_for_status()
                    data = response.json()
                    # Adicione esta linha para imprimir os dados da API do Blip
                    print("Data from Blip API:", data)
                    if 'resource' not in data:
                        raise HTTPException(
                            status_code=500, detail="Resposta da API não contém a chave 'resource'.")
                    items = data["resource"]["items"]

                    total_interacoes = sum(item["count"] for item in items)
                    chatbot_name = chatbots[i]["chatbot"]
                    total_interacoes_geral += total_interacoes
                    chatbots[i]["Total-Interacoes-Fluxo"] = total_interacoes

                except httpx.HTTPStatusError as e:
                    error_message = f"Erro na requisição para {chatbots[i]['chatbot']}: {e}"
                    print(error_message)
                    total_interacoes_geral = None
                    break  # Interrompe o loop em caso de falha

            if total_interacoes_geral is not None:
                results = {"Total_Interacoes": total_interacoes_geral, "chatbots": chatbots}
                print("Sucesso no processamento do helper-interaction")
                return results
            else:
                print(f"Tentativa {retry + 1} de {max_retries} falhou. Tentando novamente...")

        results = {"error_message": "Falhou na requisição Take após várias tentativas"}
        return results

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro de processamento interaction: Não foi possível processar os dados corretamente vindo da Take. Tente novamente")

# Rota para calcular a porcentagem de retenção
@app.post("/percentage", status_code=status.HTTP_200_OK)
async def calculate_percentage(data: dict = Body(...)):
    total_retention = float(data.get("retention"))
    total_interaction = float(data.get("interaction"))
    month = data.get("month")
    year = data.get("year")

    if total_retention is None or total_interaction is None or month is None or year is None:
        raise HTTPException(
            status_code=400, detail="Os valores de 'retention', 'interaction', 'month' e 'year' são obrigatórios.")

    # Conexão com a coleção "earnings"
    earnings_collection = db["earnings"]

    # Verificar se já existe um registro para o mesmo mês e ano
    existing_record = earnings_collection.find_one({"month": month, "year": year})
    if existing_record:
        raise HTTPException(
            status_code=409, detail=f"Já existe um registro para o mês {month} e ano {year}.")

    try:
        # Calcular a porcentagem de retenção
        porcentagem_retention = (total_retention * 100) / total_interaction

        # Formatar os valores
        formatted_porcentagem_retention = "{:.2f}".format(porcentagem_retention) + "%"  # Ajuste aqui
        formatted_total_retention = "{:.0f}".format(total_retention)
        formatted_total_interaction = "{:.0f}".format(total_interaction)

        # Criar um dicionário para o resultado final
        result = {
            "porcentagem_retention": formatted_porcentagem_retention,
            "total_retention": formatted_total_retention,
            "total_interaction": formatted_total_interaction
        }

        # Salvar os dados na coleção "earnings" somente se não existir registro para o mesmo mês e ano
        earnings_data = {
            "year": year,
            "month": month,
            "total_retention": formatted_total_retention,
            "total_interaction": formatted_total_interaction,
            "porcentagem_retention": formatted_porcentagem_retention
        }
        earnings_collection.insert_one(earnings_data)

        return result

    except ZeroDivisionError:
        raise HTTPException(
            status_code=400, detail="O valor de 'retention' não pode ser zero.")
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro desconhecido: {exc}")

@app.get("/get-percentage-earning")
async def get_percentage_earning(month: str, year: str):
    # Conexão com a coleção "earnings"
    earnings_collection = db["earnings"]

    try:
        # Buscar os dados na coleção "earnings" pelo mês e ano especificados
        earnings_data = earnings_collection.find_one({"month": month, "year": year})

        if not earnings_data:
            raise HTTPException(
                status_code=404, detail=f"Dados para o mês {month} e ano {year} não encontrados.")

        # Converter o objeto ObjectId para uma string serializável
        earnings_data["_id"] = str(earnings_data["_id"])

        return earnings_data

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro comunicação {exc}")
