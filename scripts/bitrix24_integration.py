"""
Модуль интеграции с Битрикс24 CRM
Отправляет лиды и прикрепленные файлы в CRM через REST API
"""
import os
import logging
import asyncio
from typing import Optional, Dict, Any, List
from pathlib import Path
import httpx
import aiofiles
from datetime import datetime
import base64

logger = logging.getLogger(__name__)


class Bitrix24Client:
    """Клиент для работы с Битрикс24 REST API"""
    
    def __init__(self, webhook_url: str):
        """
        Инициализация клиента
        
        Args:
            webhook_url: URL вебхука вида https://your-domain.bitrix24.ru/rest/user_id/webhook_code/
        """
        self.webhook_url = webhook_url.rstrip('/')
        self.session = None
    
    async def __aenter__(self):
        """Создание HTTP клиента"""
        self.session = httpx.AsyncClient(
            timeout=30.0,
            headers={'User-Agent': 'RostFerrum-Site/1.0'}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие HTTP клиента"""
        if self.session:
            await self.session.aclose()
    
    async def _make_request(self, method: str, endpoint: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Выполнение запроса к API
        
        Args:
            method: HTTP метод (GET, POST)
            endpoint: Эндпоинт API (например, 'crm.lead.add.json')
            data: Данные для отправки
            
        Returns:
            Ответ от API в формате dict
            
        Raises:
            httpx.RequestError: Ошибка HTTP запроса
            ValueError: Ошибка API Битрикс24
        """
        url = f"{self.webhook_url}/{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = await self.session.get(url, params=data)
                result = response.json()
            else:
                # Для POST используем form-data и корректно разворачиваем вложенные структуры
                form_data: Dict[str, str] = {}

                def add_field(prefix: str, value: Any) -> None:
                    if isinstance(value, dict):
                        for dk, dv in value.items():
                            add_field(f"{prefix}[{dk}]", dv)
                    elif isinstance(value, list):
                        for i, item in enumerate(value):
                            add_field(f"{prefix}[{i}]", item)
                    elif value is None:
                        # Битрикс24 не любит пустые значения
                        return
                    else:
                        form_data[prefix] = str(value)

                if data:
                    for key, value in data.items():
                        add_field(key, value)

                response = await self.session.post(url, data=form_data)
                result = response.json()
            
            # Проверка на ошибки API
            if 'error' in result:
                error_msg = result.get('error_description', result.get('error', 'Unknown API error'))
                logger.error(f"Bitrix24 API error: {error_msg}")
                raise ValueError(f"Bitrix24 API error: {error_msg}")
            
            return result
            
        except httpx.RequestError as e:
            logger.error(f"HTTP request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in API request: {e}")
            raise
    
    async def create_lead(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Создание лида в CRM
        
        Args:
            lead_data: Данные лида в формате Битрикс24
            
        Returns:
            Результат создания лида с ID
        """
        logger.info(f"Creating lead in Bitrix24: {lead_data.get('fields', {}).get('TITLE', 'No title')}")
        
        result = await self._make_request('POST', 'crm.lead.add.json', lead_data)
        
        if result.get('result'):
            lead_id = result['result']
            logger.info(f"Lead created successfully with ID: {lead_id}")
            return {'success': True, 'lead_id': lead_id, 'response': result}
        else:
            logger.error(f"Failed to create lead: {result}")
            return {'success': False, 'error': 'Failed to create lead', 'response': result}
    
    async def upload_file(self, file_path: str, file_name: str = None) -> Optional[str]:
        """
        Загрузка файла в Битрикс24
        
        Args:
            file_path: Путь к файлу
            file_name: Имя файла (если не указано, берется из пути)
            
        Returns:
            ID загруженного файла или None в случае ошибки
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        
        if not file_name:
            file_name = os.path.basename(file_path)
        
        try:
            # Читаем файл
            async with aiofiles.open(file_path, 'rb') as f:
                file_content = await f.read()
            
            # Подготавливаем данные для загрузки
            files = {'fileContent': (file_name, file_content)}
            
            url = f"{self.webhook_url}/disk.folder.uploadfile.json"
            
            # Сначала получаем корневую папку пользователя
            folder_result = await self._make_request('GET', 'disk.storage.getlist.json')
            if not folder_result.get('result'):
                logger.error("Failed to get storage list")
                return None
            
            # Берем первое хранилище (обычно это "Мой диск")
            storage_id = folder_result['result'][0]['ID']
            
            # Загружаем файл в корневую папку хранилища
            upload_data = {
                'id': storage_id,
                'data[NAME]': file_name
            }
            
            # Используем files для загрузки файла
            response = await self.session.post(
                f"{self.webhook_url}/disk.storage.uploadfile.json",
                data=upload_data,
                files=files
            )
            result = response.json()
            
            if result.get('result'):
                file_id = result['result']['ID']
                logger.info(f"File uploaded successfully with ID: {file_id}")
                return file_id
            else:
                logger.error(f"Failed to upload file: {result}")
                return None
                
        except Exception as e:
            logger.error(f"Error uploading file {file_path}: {e}")
            return None
    
    async def attach_file_to_lead(self, lead_id: str, file_id: str) -> bool:
        """
        Прикрепление файла к лиду
        
        Args:
            lead_id: ID лида
            file_id: ID загруженного файла
            
        Returns:
            True если файл прикреплен успешно
        """
        try:
            attach_data = {
                'id': lead_id,
                'fields': {
                    'UF_CRM_TASK': file_id  # Стандартное поле для файлов
                }
            }
            
            result = await self._make_request('POST', 'crm.lead.update.json', attach_data)
            
            if result.get('result'):
                logger.info(f"File {file_id} attached to lead {lead_id}")
                return True
            else:
                logger.error(f"Failed to attach file to lead: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Error attaching file to lead: {e}")
            return False


class LeadConverter:
    """Конвертер данных из формы сайта в формат Битрикс24"""
    
    @staticmethod
    def convert_lead_data(form_data: Dict[str, Any], source: str = "Сайт РостФеррум") -> Dict[str, Any]:
        """
        Конвертация данных формы в формат лида Битрикс24
        
        Args:
            form_data: Данные из формы сайта
            source: Источник лида
            
        Returns:
            Данные лида в формате Битрикс24
        """
        # Читаем ответственного из ENV (если задан)
        responsible_env = os.getenv('BITRIX24_RESPONSIBLE_USER_ID')
        try:
            responsible_id = int(responsible_env) if responsible_env else 1
        except Exception:
            responsible_id = 1

        # Пользовательские поля из ENV (с дефолтами из запроса пользователя)
        file_field_code = os.getenv('BITRIX24_FILE_FIELD_ID', 'UF_CRM_1757503960')
        comment_field_code = os.getenv('BITRIX24_COMMENT_FIELD_ID', 'UF_CRM_1757505050')

        # Базовые поля лида
        fields = {
            'TITLE': f"Заявка с сайта - {form_data.get('name', 'Без имени')}",
            'NAME': form_data.get('name', ''),
            'PHONE': [{'VALUE': form_data.get('phone', ''), 'VALUE_TYPE': 'WORK'}] if form_data.get('phone') else [],
            'EMAIL': [{'VALUE': form_data.get('email', ''), 'VALUE_TYPE': 'WORK'}] if form_data.get('email') else [],
            # Дублируем комментарий и в стандартное поле, и в пользовательское
            'COMMENTS': form_data.get('message', ''),
            'SOURCE_ID': 'WEB',  # Источник - веб-сайт
            'SOURCE_DESCRIPTION': source,
            'OPENED': 'Y',  # Лид доступен всем
            'ASSIGNED_BY_ID': responsible_id,  # ID ответственного
        }
        
        # Добавляем дополнительные поля если есть
        if form_data.get('product_id'):
            fields['COMMENTS'] += f"\n\nТовар: {form_data.get('product_id')}"
        
        if form_data.get('utm_source'):
            fields['UTM_SOURCE'] = form_data.get('utm_source')
        
        if form_data.get('utm_medium'):
            fields['UTM_MEDIUM'] = form_data.get('utm_medium')
        
        if form_data.get('utm_campaign'):
            fields['UTM_CAMPAIGN'] = form_data.get('utm_campaign')
        
        # Пользовательское поле для комментария
        if form_data.get('message'):
            fields[comment_field_code] = form_data.get('message')

        # Файл добавляется на этапе send_lead_to_bitrix24 (после base64)

        return {'fields': fields}


async def send_lead_to_bitrix24(
    form_data: Dict[str, Any], 
    file_path: Optional[str] = None,
    file_bytes: Optional[bytes] = None,
    file_name: Optional[str] = None,
    webhook_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Отправка лида в Битрикс24 CRM
    
    Args:
        form_data: Данные из формы
        file_path: Путь к прикрепленному файлу
        webhook_url: URL вебхука (если не указан, берется из переменных окружения)
        
    Returns:
        Результат отправки
    """
    if not webhook_url:
        webhook_url = os.getenv('BITRIX24_WEBHOOK_URL')
    
    if not webhook_url:
        logger.error("Bitrix24 webhook URL not configured")
        return {'success': False, 'error': 'Bitrix24 not configured'}
    
    try:
        # Конвертируем данные формы
        converter = LeadConverter()
        lead_data = converter.convert_lead_data(form_data)

        # Подготовка файла: поддержка как пути на диске, так и байтов в памяти
        file_included = False
        if file_bytes is not None and (file_name or file_path):
            try:
                resolved_name = file_name or (Path(file_path).name if file_path else 'attachment')
                b64 = base64.b64encode(file_bytes).decode('utf-8')
                file_field_code = os.getenv('BITRIX24_FILE_FIELD_ID', 'UF_CRM_1757503960')
                lead_fields = lead_data.setdefault('fields', {})
                lead_fields[file_field_code] = {
                    'fileData': [resolved_name, b64]
                }
                file_included = True
            except Exception as e:
                logger.error(f"Failed to prepare base64 file from memory: {e}")
        elif file_path and os.path.exists(file_path):
            try:
                async with aiofiles.open(file_path, 'rb') as f:
                    binary = await f.read()
                b64 = base64.b64encode(binary).decode('utf-8')
                resolved_name = file_name or Path(file_path).name
                file_field_code = os.getenv('BITRIX24_FILE_FIELD_ID', 'UF_CRM_1757503960')
                lead_fields = lead_data.setdefault('fields', {})
                lead_fields[file_field_code] = {
                    'fileData': [resolved_name, b64]
                }
                file_included = True
            except Exception as e:
                logger.error(f"Failed to prepare base64 file from disk: {e}")

        async with Bitrix24Client(webhook_url) as client:
            # Создаем лид (с уже вложенным base64-файлом, если есть)
            lead_result = await client.create_lead(lead_data)
            if not lead_result['success']:
                return lead_result
            return {
                'success': True,
                'lead_id': lead_result['lead_id'],
                'file_attached': file_included
            }
            
    except Exception as e:
        logger.error(f"Error sending lead to Bitrix24: {e}")
        return {'success': False, 'error': str(e)}


# Функция для тестирования интеграции
async def test_bitrix24_integration():
    """Тестирование интеграции с Битрикс24"""
    test_data = {
        'name': 'Тестовый клиент',
        'phone': '+7 (999) 123-45-67',
        'email': 'test@example.com',
        'message': 'Тестовая заявка для проверки интеграции'
    }
    
    result = await send_lead_to_bitrix24(test_data)
    print(f"Test result: {result}")
    return result


if __name__ == "__main__":
    # Запуск теста
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_bitrix24_integration())
