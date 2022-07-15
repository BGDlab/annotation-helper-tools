useState = React.useState
el = React.createElement

def App():
    val, setVal = useState("")


    def load_file():
        pass

    def save_file():
        pass

    def say_hello():
        setVal("Hello React")

    def clear_it():
        setVal("") 

    return [
            el('button', {'onClick': say_hello}, "Click Me"),
            el('button', {'onClick': clear_it}, "Clear"),
            el('div', None, annotatedCount),
            el('div', None, val)
    ]

def render():
    ReactDOM.render(
            el(App, None), 
            document.getElementById('root')
    )

document.addEventListener('DOMContentLoaded', render)

