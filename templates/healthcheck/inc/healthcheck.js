function process_messages(messages) {
    if (typeof messages === "string") {
        messages = [messages]
    }
    errorObj = document.getElementById("errors")
    if (errorObj) {
        errorObj.innerHTML = "<li>" + messages.join("</li><li>") + "</li>"
    } else {
        console.log(messages.join("\n"));
    }
}
function clear_messages(messages) {
    errorObj = document.getElementById("errors")
    if (errorObj) {
        errorObj.innerHTML = ""
        errorObj.style.display = "none"
    }

}
async function start_preview(){
    try {
        document.getElementById("runningstatus").innerHTML="Waiting..."
        document.getElementById("button_start").disabled = true
        document.getElementById("button_stop").disabled = true
        const response = await fetch("/healthcheck/config/preview/start")
        body = await response.text()
        if (!response.ok) {
            document.getElementById("runningstatus").innerHTML="Stopped..."
            document.getElementById("button_start").disabled = false
            throw new Error(response.status + " : " + body);
        } else {
            running = true
            document.getElementById("runningstatus").innerHTML="Running..."
            document.getElementById("button_stop").disabled = false
            if (!fetching) {
                fetch_healthstatus()
            }
        }

        clear_messages()
        return body
    } catch(error) {
        process_messages(error.message)
    }
}


async function stop_preview(){
    try {
        document.getElementById("runningstatus").innerHTML="Waiting..."
        document.getElementById("button_start").disabled = true
        document.getElementById("button_stop").disabled = true
        const response = await fetch("/healthcheck/config/preview/stop")
        body = await response.text()
        if (!response.ok) {
            document.getElementById("runningstatus").innerHTML="Running..."
            document.getElementById("button_stop").disabled = false
            throw new Error(response.status + " : " + body);
        } else {
            if (controller !== null) {
                controller.abort()
            }
            document.getElementById("runningstatus").innerHTML="Stopped..."
            document.getElementById("button_start").disabled = false
            running = false
        }
        return body
    } catch(error) {
        process_messages(error.message)
     }
}


async function reload_dashboard(){
    try {
        const response = await fetch("/healthcheck/reload")
        body = await response.text()
        if (!response.ok) {
            throw new Error(response.status + " : " + body);
        }
        return body
    } catch(error) {
        process_messages(error.message)
     }
}
