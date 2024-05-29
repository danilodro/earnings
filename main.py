from fastapi import FastAPI, HTTPException, Body, status
from pymongo import MongoClient
import httpx
import asyncio
from collections import defaultdict
from datetime import datetime
import calendar
from aiocache import Cache

app = FastAPI()

# Conexão com o banco de dados MongoDB
client = MongoClient(
    "mongodb+srv://chatbotTake:blipAutomation@cluster0.qm1oi6z.mongodb.net/")
db = client["earnings-Take"]
collection = db["chatbot-earnings"]

# Configuração do cache
cache = Cache(Cache.MEMORY)

# Rota para adicionar chatbot e key
@app.post("/add-chatbot", status_code=status.HTTP_201_CREATED)
async def add_chatbot(chatbot: str = Body(...), key: str = Body(...)):
    if collection.find_one({"chatbot": chatbot}):
        raise HTTPException(
            status_code=400, detail="Chatbot já existe no banco de dados.")
    result = collection.insert_one({"chatbot": chatbot, "key": key})
    return {"chatbot": chatbot, "key": key}

# Rota para buscar chatbot e key
@app.post("/search-chatbot")
async def search_chatbot(chatbot_data: dict = Body(...)):
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
    if not collection.find_one({"chatbot": chatbot}):
        raise HTTPException(
            status_code=404, detail="Chatbot não encontrado no banco de dados.")
    collection.update_one({"chatbot": chatbot}, {"$set": {"key": key}})
    return {"message": "Chatbot atualizado com sucesso.", "chatbot": chatbot, "key": key}

# Rota para excluir chatbot
@app.delete("/delete-chatbot")
async def delete_chatbot(chatbot: str):
    if not collection.find_one({"chatbot": chatbot}):
        raise HTTPException(
            status_code=404, detail="Chatbot não encontrado no banco de dados.")
    collection.delete_one({"chatbot": chatbot})
    return {"message": "Chatbot excluído com sucesso.", "chatbot": chatbot}

def get_chatbots_from_db():
    chatbots_data = collection.find({}, {"_id": 0, "chatbot": 1, "key": 1})
    chatbots = [{"chatbot": chatbot["chatbot"], "key": chatbot["key"]}
                for chatbot in chatbots_data]
    return chatbots

@app.get("/get-chatbots")
async def get_chatbots():
    chatbots_data = collection.find({}, {"_id": 0, "chatbot": 1, "key": 1})
    chatbots = [{"chatbot": chatbot["chatbot"], "key": chatbot["key"]}
                for chatbot in chatbots_data]
    return {"chatbots": chatbots}

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
    return difference + 1

async def fetch_data(chatbots, uri):
    semaphore = asyncio.Semaphore(10)  # Limitar a 10 chamadas simultâneas
    tasks = []

    async def fetch(chatbot):
        async with semaphore:
            key = chatbot.get("key")
            if key:
                headers = {"Authorization": key}
                return await make_request("https://intelbras.http.msging.net/commands", headers, {
                    "id": "444", "to": "postmaster@analytics.msging.net", "method": "get", "uri": uri})

    for chatbot in chatbots:
        tasks.append(fetch(chatbot))

    return await asyncio.gather(*tasks)

@app.post("/helper-events")
async def helper_events(event_data: dict = Body(...)):
    try:
        month = event_data.get("month")
        if not month:
            raise HTTPException(
                status_code=400, detail="Campo 'month' é obrigatório.")

        chatbots = get_chatbots_from_db()
        if not chatbots:
            raise HTTPException(
                status_code=404, detail="Nenhum chatbot encontrado no banco de dados.")

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

        uri = f"/event-track/Metricas-Fluxo?startDate={start_date}&endDate={end_date}&$take=10000"

        responses = await fetch_data(chatbots, uri)

        results = {}
        total = defaultdict(int)

        for i, response in enumerate(responses):
            try:
                response.raise_for_status()
                data = response.json()
                items = data["resource"]["items"]

                total_retidos = 0
                for item in items:
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
                continue

            except Exception as e:
                error_message = f"Erro desconhecido para {chatbots[i]['chatbot']}: {e}"
                print(error_message)
                continue

        total_result = {
            "Retidos_bot": sum([item.get("Retidos", 0) for item in chatbots]),
            "start_date": start_date,
            "end_date": end_date
        }

        results.update({chatbot["chatbot"]: chatbot for chatbot in chatbots})
        results["total"] = total_result

        return results

    except Exception as exc:
        error_message = f"Erro de processamento events: Não foi possível processar os dados corretamente vindo da Take. Tente novamente: {exc}"
        print(error_message)
        raise HTTPException(
            status_code=500, detail=error_message)

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

        responses = await fetch_data(chatbots, uri)

        total_interacoes_geral = 0

        for i, response in enumerate(responses):
            try:
                response.raise_for_status()
                data = response.json()
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
                break

        if total_interacoes_geral is not None:
            results = {"Total_Interacoes": total_interacoes_geral, "chatbots": chatbots}
            return results
        else:
            raise HTTPException(
                status_code=500, detail="Falhou na requisição Take após várias tentativas")

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro de processamento interaction: Não foi possível processar os dados corretamente vindo da Take. Tente novamente")
