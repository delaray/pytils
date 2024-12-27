from typing import Dict, Union, Optional
import ollama
from ollama import chat
from ollama import ChatResponse
from utils.utils import timing
import base64
import os


def encode_image(image_path):
    "Getting the base64 string"
    
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


# --------------------------------------------------------------------------------------
# Basic Ollama Chat
# --------------------------------------------------------------------------------------

@timing
def ollama_chat(question, model='phi', verbose=False):
    response: ChatResponse = chat(model=model, messages=[
        {'role': 'user',  'content': question}])

    if verbose:
        print(f"\nResponse: {response['message']['content']}\n")

    return response


# --------------------------------------------------------------------------------------
# Basic Olla
# --------------------------------------------------------------------------------------

# NB: Code written by CLAUDE. Gets an error with the temperature
# argument. Need to debug....
@timing
def query_ollama(prompt: str,  model: str = "phi",
                 temperature: float = 0.7,
                 system_prompt: Optional[str] = None,
                 host: str = "http://localhost:11434"
                 ) -> Dict[str, Union[str, int]]:
    
    """
    Query an Ollama model using the official Python client and return the response 
    along with token usage statistics.
    
    Args:
        prompt (str): The user's question or prompt
        model (str, optional): The name of the Ollama model to use. Defaults to "phi".
        temperature (float, optional): Controls randomness in response. Defaults to 0.7.
        system_prompt (str, optional): System prompt to set context. Defaults to None.
        host (str, optional): Ollama API host. Defaults to "http://localhost:11434".
    
    Returns:
        Dict[str, Union[str, int]]: Dictionary containing:
            - 'response': The model's response text
            - 'prompt_tokens': Number of tokens in the prompt
            - 'completion_tokens': Number of tokens in the completion
            - 'total_tokens': Total tokens used
    
    Raises:
        ollama.ResponseError: If there's an error from the Ollama API
        Exception: For other unexpected errors
    """
    
    try:
        # Configure the client with custom host if provided
        client = ollama.Client(host=host)
        
        # Prepare the generation parameters
        params = {
            "model": model,
            "temperature": temperature,
            "prompt": prompt
            
        }
        
        # Add system prompt if provided
        if system_prompt:
            params["system"] = system_prompt
            
        # Generate the response
        response = client.generate(**params)
        
        # Extract token counts and response
        result = {
            "response": response['response'],
            "prompt_tokens": response.get('prompt_eval_count', 0),
            "completion_tokens": response.get('eval_count', 0)}
        
        # Calculate total tokens
        result["total_tokens"] = result["prompt_tokens"] + result["completion_tokens"]
        
        return result
        
    except ollama.ResponseError as e:
        raise ollama.ResponseError(f"Ollama API error: {str(e)}")
    
    except Exception as e:
        raise Exception(f"Unexpected error while querying Ollama: {str(e)}")

# Example usage
if __name__ == "__main__":
    try:
        # Make sure you have installed the ollama package: pip install ollama
        # And have pulled the phi model: ollama pull phi
        
        result = query_ollama(
            prompt="What is the capital of France?",
            model="phi",
            temperature=0.7,
            system_prompt="You are a helpful assistant."
        )
        
        print("Response:", result["response"])
        print("\nToken usage:")
        for key, value in result.items():
            if key != "response":
                print(f"{key}: {value}")
                
    except Exception as e:
        print(f"Error: {str(e)}")
