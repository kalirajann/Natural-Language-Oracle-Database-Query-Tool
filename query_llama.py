import requests
import json

def query_ollama(prompt, model="llama3.2:latest"):
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            response_data = response.json()
            print("Full response:", json.dumps(response_data, indent=2))  # Debug print
            return response_data.get('response', '')
        else:
            print(f"Error: Received status code {response.status_code}")
            print(f"Response: {response.text}")
            return f"Error: Failed to get response from Ollama (Status code: {response.status_code})"
    except requests.exceptions.ConnectionError:
        return "Error: Could not connect to Ollama. Make sure Ollama is running on localhost:11434"
    except Exception as e:
        return f"Error: {str(e)}"

# Example usage
print("Sending query to Ollama...")
output = query_ollama("What is the capital of France?")
print("\nResponse from Ollama:")
print(output)
