from pyreact import useState, render, createElement as el

def ListItems(props):
    items = props['items']
    return [el('li', {'key': item}, item) for item in items]

def App():
    # Initialize the variables in the app
    newItem, setNewItem = useState("")
    items, setItems = useState([])

    def handleSubmit(event):
        event.preventDefault()
        setItems(items + [newItem]) # add a new item to the items list
        setNewItem("") # reset the state of the newItem element?

    def handleChange(event):
        # when the item is set by the user?
        target = event['target']
        setNewItem(target['value'])

    return el('form', {'onSubmit', handleSubmit},
            el('label', {'htmlFor': 'newItem'}, "New Item: "),
            el('input', {'id': 'newItemm', 
                         'onChange': handleChange,
                         'value': newItem
                         }
              ),
            el('input', {'type': 'submit'}),
            el('ol', None, el(ListItems, {'items': items}))
            )

render(App, None, 'root')
