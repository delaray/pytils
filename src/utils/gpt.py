# **************************************************************************
# OpenAI basic ChatGPT API
# **************************************************************************

import os
import openai
import json
from datetime import datetime
import pandas as pd

# GCP Imports
import vertexai
from vertexai.language_models import TextGenerationModel

# Project Imports
from utils.storage import load_data, save_data

# --------------------------------------------------------------------------
# OPENAI API Parameters
# --------------------------------------------------------------------------

OPENAI_API_KEY =  os.getenv("OPENAI_API_KEY")
OPENAI_API_MODEL = os.getenv("OPENAI_API_MODEL", 'gpt-3.5-turbo')
OPENAI_API_USER = os.getenv("OPENAI_API_USER", 'unknown')


# **************************************************************************
# OpenAI API
# **************************************************************************

# --------------------------------------------------------------------------
# Initialize OPENAI API Parameters
# --------------------------------------------------------------------------

def init_openai(api_key=OPENAI_API_KEY, api_model=OPENAI_API_MODEL):
    'Initializes the OPENAI APIs key, organization and model.'
    openai.api_key = api_key
    openai.model = api_model
    return True


# --------------------------------------------------------------------------
# Extract Answer
# --------------------------------------------------------------------------

def extract_answer(completion):
    'Returns a dictionary of essential OPENAI API completion components.'

    answer = completion["choices"][0]["message"]["content"]
    model = completion["model"]
    usage = completion["usage"]
    prompt_tokens = usage["prompt_tokens"]
    completion_tokens = usage["completion_tokens"]
    total_tokens = usage["total_tokens"]

    return {'response' : answer,
            'model': model,
            'total_tokens': total_tokens,
            'prompt_tokens': prompt_tokens,
            'completion_tokens': completion_tokens}


# **************************************************************************
# OPENAI Token Usage Logging
# **************************************************************************

TOKENS_USAGE_LOG = f'openai-tokens-usage-{OPENAI_API_USER}'

TOKENS_USAGE_COLUMNS = ['user', 'prompt', 'response', 'prompt_tokens',
                        'completion_tokens', 'total_tokens', 'date']


# --------------------------------------------------------------------------
# Get Token Usage Log
# --------------------------------------------------------------------------

def get_openai_token_usage(bu, source='file', storage='misc',
                           folder='tokens'):
    'Returns a DataFrame of the complete token usage history.'

    # Load current token usage log
    df = load_data(bu, TOKENS_USAGE_LOG, 'csv', source,
                   storage=storage, folder=folder)

    return df

# --------------------------------------------------------------------------
# Save Token Usage DataFrame
# --------------------------------------------------------------------------

# NB: This is different from log_openai_token_usage in that the DF is
# already constructed.

def save_openai_token_usage(bu, df, source='file', storage='misc',
                            folder='tokens'):
    'Saves the specified DataFrame of token usage history.'

    # Ensure consistent & compliant column schema
    df = df[TOKENS_USAGE_COLUMNS]

    # Save the dataframe
    df = save_data(bu, TOKENS_USAGE_LOG, 'csv', source, df,
                   storage=storage, folder=folder)

    return df

# --------------------------------------------------------------------------
# Log New Token Usage
# --------------------------------------------------------------------------

# NB: This is different from save_openai_token_usage in that it retrieves
# the current log, adds one new row and then writes thew current log.

def log_openai_token_usage(bu, user, prompt, response, source='file',
                           destination='file', storage='misc',
                           folder='tokens'):

    # Get current token usage log
    df1 = get_openai_token_usage(bu, source, storage=storage, folder=folder)

    # Prepare the new row
    prompt_tokens = response['prompt_tokens']
    completion_tokens = response['completion_tokens'],
    total_tokens = response['total_tokens'],
    response = response['response']
    today = datetime.today().strftime('%Y-%m-%d')
    new_row = {'user': user,
               'prompt': prompt,
               'response': response,
               'prompt_tokens': prompt_tokens,
               'completion_tokens': completion_tokens,
               'total_tokens': total_tokens,
               'date': today}

    # Add new row and save token consumption log
    df2 = pd.DataFrame([new_row], columns=df1.columns)
    df = pd.concat([df1, df2], axis=0)
    save_data(bu, TOKENS_USAGE_LOG, 'csv', df, destination,
              storage=storage, folder=folder)

    return df


# --------------------------------------------------------------------------
# Reset Token Usage Log
# --------------------------------------------------------------------------

def reset_openai_token_usage(bu, destination='file', storage='misc',
                             folder='tokens', initial_rows=[]):
    'Initializes or resets the OpenAI tokens usage log file.'

    df = pd.DataFrame(initial_rows, columns=TOKENS_USAGE_COLUMNS)
    save_data(bu, TOKENS_USAGE_LOG, 'csv', df, destination=destination,
              storage=storage, folder=folder)

    return df


# **************************************************************************
# OPENAI Completion API
# **************************************************************************

# --------------------------------------------------------------------------
# Parse Prompt
# --------------------------------------------------------------------------

def parse_prompt(prompt: str, role='user'):
    return [{"role": role, "content": prompt}]


# --------------------------------------------------------------------------
# Complete Prompt
# --------------------------------------------------------------------------

def _openai_chat_complete(user, messages, model, temperature, max_tokens,
                          storage, folder):
    try:
        completion = openai.ChatCompletion.create(model=model, messages=messages,
                                                  temperature=temperature,
                                                  max_tokens=max_tokens)
        answer = extract_answer(completion)
        # log_openai_token_usage('lmfr', user, messages, answer, storage=storage,
        #                        folder=folder)

        result = {'user': user, 'prompt': messages}
        result.update(answer)

        return result

    except Exception as err:
        print(f'\nError calling OpenAI API:\n{err}')
        print(f'\nRetrying call to complete_prompt...\n')
        if err == 'The server is overloaded or not ready yet.':
            return _openai_chat_complete(user, messages, model, temperature,
                                         max_tokens, storage, folder)
        else:
            return None


def complete_user_prompt(user, prompt, temperature=0.8, model=OPENAI_API_MODEL,
                         max_tokens=1000, storage='misc', folder='tokens'):
    'Complete prompt using a user role in a standard chat style prompt.'

    messages = parse_prompt(prompt)

    return _openai_chat_complete(user, messages, model,
                                 temperature, max_tokens, storage, folder)


def complete_system_prompt(user, prompt, system_instruction, temperature=0.8,
                           model=OPENAI_API_MODEL, max_tokens=1000,
                           storage='misc', folder='tokens'):
    'Complete prompt using a system role with output format instructions.'

    messages = parse_prompt(prompt)
    messages.extend(parse_prompt(system_instruction role='system'))

    return _openai_chat_complete(user, messages, model,
                                 temperature, max_tokens, storage, folder)


def complete_prompt(user, prompt, temperature=0.8, model=OPENAI_API_MODEL,
                    max_tokens=1000, storage='misc', folder='tokens'):
    return complete_use_prompt(user, prompt, temperature=temperature,
                               model=model, max_tokens=max_tokens,
                               storage=storage, folder=folder)


def test_completion(country='France', user=OPENAI_API_USER,
                    storage='misc', folder='tokens'):
    init_openai()
    text = f"What is the capital of {country}"
    return complete_user_prompt(user, text, storage=storage, folder=folder)


# --------------------------------------------------------------------------
# Function Calling API
# --------------------------------------------------------------------------

# TODO: Make use of of OPENAI function calling facility.


# ****************************************************************************
# Vertex AI
# ****************************************************************************

# vertexai.init(project="opus-dev-hmbu-cdp", location="us-central1")

VERTEXAI_MODEL = None

def init_vertexai():
    vertexai.init()
    global VERTEXAI_MODEL
    VERTEXAI_MODEL = TextGenerationModel.from_pretrained("text-bison@001")
    return True


# --------------------------------------------------------------------------

def vertexai_user_prompt(prompt: str, temperature=0, max_tokens=1024):

    model = VERTEXAI_MODEL

    parameters = {"max_output_tokens": max_tokens,
                  "temperature": temperature,
                  "top_p": 0.8,
                  "top_k": 40}

    try:
        response = model.predict(prompt, **parameters)
        return response

    except Exception as err:
        print(f'\nError calling VertexAI API:\n{err}\n')
        return None


# "candidate_count": 1,


# **************************************************************************
# End of File
# **************************************************************************
