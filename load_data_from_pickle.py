import pickle

try:
    with open('data.pickle', 'rb') as file:
        data = pickle.load(file)
    
    print("Pickle file loaded successfully!")
    print(f"Data type: {type(data)}")
    
    if hasattr(data, '__len__'):
        print(f"Length: {len(data)}")
    
    print(f"Data content: {data}")
    
except FileNotFoundError:
    print("Error: data.pickle file not found")
except Exception as e:
    print(f"Error loading pickle: {e}")