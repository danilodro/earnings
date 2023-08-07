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

# Função para realizar a requisição POST para a URL específica com a chave de autorização


async def make_request(url, headers, data):
    try:
        async with httpx.AsyncClient() as client:
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
        # Obter todas as chaves de chatbots
        chatbots = get_chatbots_from_db()
        if not chatbots:
            raise HTTPException(
                status_code=404, detail="Nenhum chatbot encontrado no banco de dados.")

        # Lista para armazenar as tarefas de requisição assíncrona
        tasks = []

        # Realizar a requisição para cada chave de chatbot
        for chatbot in chatbots:
            key = chatbot.get("key")
            if key:
                headers = {"Authorization": key}
                task = asyncio.create_task(make_request(
                    "https://intelbras.http.msging.net/commands", headers, event_data))
                tasks.append(task)

        # Aguardar todas as requisições serem concluídas
        responses = await asyncio.gather(*tasks)

        # Verificar se todas as requisições foram bem-sucedidas
        for response in responses:
            if response.status_code != status.HTTP_200_OK:
                raise HTTPException(status_code=response.status_code,
                                    detail="Erro ao fazer a requisição com as chaves.")

        # Processar as respostas e calcular os totais por ação para cada chatbot
        results = {}
        # Inicializa um defaultdict para armazenar o total de cada ação em todos os chatbots
        total = defaultdict(int)

        for i, response in enumerate(responses):
            chatbot_name = chatbots[i]["chatbot"]
            data = response.json()
            items = data["resource"]["items"]

            totals = {}
            for item in items:
                action = item["action"]
                count = item["count"]
                totals[action] = count

            # Adicionar a data formatada na resposta
            metadata = data.get("metadata", {})
            uri = metadata.get("#command.uri", "")
            start_date = uri.split("startDate=")[1].split("&")[0]
            end_date = uri.split("endDate=")[1]
            formatted_date = f"{start_date} até {end_date}"

            # Adicionar os totais e a data formatada ao resultado
            result = {**totals, "Data": formatted_date}
            results[chatbot_name] = result

            # Atualizar o total de cada ação em todos os chatbots
            for action, count in totals.items():
                total[action] += count

        # Adiciona o total de todas as ações em todos os chatbots aos resultados
        results["total dos bots"] = total

        # Calcular os totais das métricas de todos os chatbots
        total_abandono = total.get("Abandono de fluxo", 0)
        total_retidos = total.get("Retidos no bot", 0)
        total_menu_principal = total.get("Retornou ao menu principal", 0)

        # Criar um novo resultado com os totais das métricas
        total_result = {
            "Abandono de fluxo": total_abandono,
            "Retidos no bot": total_retidos,
            "Retornou ao menu principal": total_menu_principal
        }

        # Adicionar o novo resultado ao resultado geral
        results["total"] = total_result

        return results

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro desconhecido na helper-track.")

    # Rota para o helper-interaction


@app.post("/helper-interaction")
async def helper_interaction(event_data: dict = Body(...)):
    try:
        # Obter todas as chaves de chatbots
        chatbots = get_chatbots_from_db()
        if not chatbots:
            raise HTTPException(
                status_code=404, detail="Nenhum chatbot encontrado no banco de dados.")

        # Lista para armazenar as tarefas de requisição assíncrona
        tasks = []

        # Realizar a requisição para cada chave de chatbot
        for chatbot in chatbots:
            key = chatbot.get("key")
            if key:
                headers = {"Authorization": key}
                task = asyncio.create_task(make_request(
                    "https://intelbras.http.msging.net/commands", headers, event_data))
                tasks.append(task)

        # Aguardar todas as requisições serem concluídas
        responses = await asyncio.gather(*tasks)

        # Verificar se todas as requisições foram bem-sucedidas
        for response in responses:
            if response.status_code != status.HTTP_200_OK:
                raise HTTPException(status_code=response.status_code,
                                    detail="Erro ao fazer a requisição com as chaves.")

        # Processar as respostas e calcular o total de interações de fluxo para cada chatbot
        results = {}
        total_interacoes_geral = 0

        for i, response in enumerate(responses):
            chatbot_name = chatbots[i]["chatbot"]
            data = response.json()
            items = data["resource"]["items"]

            total_interacoes = sum(item["count"] for item in items)
            results[chatbot_name] = {
                "Total Interacoes-Fluxo": total_interacoes}
            total_interacoes_geral += total_interacoes

        # Adicionar o total de interações de fluxo de todos os chatbots aos resultados
        results["Total Geral"] = {
            "Total Interacoes-Fluxo": total_interacoes_geral}

        return results

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro desconhecido: helper-interaction{exc}")
    


# @app.post("/data-entry", status_code=status.HTTP_200_OK)
# async def data_entry(data: dict = Body(...)):
#     month = data.get("month")
#     if not month:
#         raise HTTPException(status_code=400, detail="Campo 'month' é obrigatório.")

#     # Dicionário para mapear o nome do mês para o número do mês
#     month_map = {
#         "janeiro": "01",
#         "fevereiro": "02",
#         "março": "03",
#         "abril": "04",
#         "maio": "05",
#         "junho": "06",
#         "julho": "07",
#         "agosto": "08",
#         "setembro": "09",
#         "outubro": "10",
#         "novembro": "11",
#         "dezembro": "12",
#     }

#     # Converter o nome do mês para o número do mês (com zero à esquerda, se necessário)
#     month = month.lower()
#     month_number = month_map.get(month)
#     if not month_number:
#         raise HTTPException(status_code=400, detail="Mês inválido. Certifique-se de digitar um mês válido em português.")

#     # Obter o primeiro e último dia do mês
#     try:
#         year = datetime.now().year  # Obtém o ano atual
#         first_day = f"{year}-{month_number}-01"
#         last_day = f"{year}-{month_number}-{calendar.monthrange(year, int(month_number))[1]}"
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Erro ao calcular o primeiro e último dia do mês: {e}")

#     # Formatar os resultados no formato solicitado
#     result = f"startDate={first_day}&endDate={last_day}"

#     try:
#         # Fazer a requisição ao helper-interaction com os dados do mês
#         interaction_data = {
#             "id": "444",
#             "to": "postmaster@analytics.msging.net",
#             "method": "get",
#             "uri": f"/event-track/Interacoes-Fluxo?{result}"
#         }
#         interaction_response = await make_request("https://intelbras.http.msging.net/commands", headers={}, data=interaction_data)
#         interaction_result = interaction_response.json()

#         # Fazer a requisição ao helper-events com os dados do mês
#         events_data = {
#             "id": "444",
#             "to": "postmaster@analytics.msging.net",
#             "method": "get",
#             "uri": f"/event-track/Retidos-no-bot?{result}"
#         }
#         events_response = await make_request("https://intelbras.http.msging.net/commands", headers={}, data=events_data)
#         events_result = events_response.json()

#         # Obter o total de interações de fluxo e o total retidos no bot diretamente dos resultados
#         interactions_total = interaction_result.get("resource", {}).get("total", {}).get("Total Interacoes-Fluxo", 0)
#         events_retidos_total = events_result.get("resource", {}).get("total", {}).get("Retidos no bot", 1)  # Usamos 1 como valor padrão para evitar divisão por zero

#         # Calcular a porcentagem de interações de fluxo retidas no bot
#         percentage = (interactions_total / events_retidos_total) * 100

#         # Criar um dicionário para o resultado final
#         final_result = {
#             "result": result,
#             "interaction_result": interaction_result,
#             "events_result": events_result,
#             "percentage": percentage
#         }

#         return final_result

#     except Exception as exc:
#         raise HTTPException(status_code=500, detail=f"Erro desconhecido: {exc}")