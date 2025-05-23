import { ReactNode } from "react"

import { Text } from '@radix-ui/themes'
import ReactJson from "react-json-view"

type ArrowType = string | { [key: string]: ArrowType }

function isList(input: string): boolean {
    return input.startsWith('List(') && input.endsWith(')')
}

function parseArrowType(input: string): ArrowType {
    // Remove all whitespace characters for easier parsing
    input = input.replace(/\s+/g, '')

    // Base cases
    if (/^[a-zA-Z0-9]+$/.test(input)) {
        return input
    }

    if (input.startsWith('List(') && input.endsWith(')')) {
        const innerType = input.slice(5, -1)
        return parseArrowType(innerType)
    }

    if (input.startsWith('Struct({') && input.endsWith('})')) {
        const fieldsStr = input.slice(8, -2)
        const fields: Record<string, ArrowType> = {}

        let level = 0
        let lastPos = 0
        let fieldName = ''
        let inFieldName = true
        let inString = false

        for (let i = 0; i < fieldsStr.length; i++) {
            const char = fieldsStr[i]
            if (inFieldName) {
                if (char === '"' && !inString) {
                    inString = true
                } else if (char === '"' && inString) {
                    inString = false
                } else if (char === ':' && !inString) {
                    fieldName = fieldsStr.slice(lastPos, i)
                    lastPos = i + 1
                    inFieldName = false
                }
            } else {
                if (char === '(' && !inString) {
                    level++
                } else if (char === ')' && !inString) {
                    level--
                } else if (char === '}' && level === 0 && !inString) {
                    const next_field = fieldsStr.slice(lastPos, i + 1)
                    fields[fieldName.slice(1, -1) + (isList(next_field) ? '[List]' : '')] = parseArrowType(next_field)
                    lastPos = i + 2
                    inFieldName = true
                } else if (char === ',' && level === 0 && !inString) {
                    const next_field = fieldsStr.slice(lastPos, i)
                    fields[fieldName.slice(1, -1) + (isList(next_field) ? '[List]' : '')] = parseArrowType(next_field)
                    lastPos = i + 1
                    inFieldName = true
                }
            }
        }
        if (lastPos < fieldsStr.length) {
            const next_field = fieldsStr.slice(lastPos)
            fields[fieldName.slice(1, -1) + (isList(next_field) ? '[List]' : '')] = parseArrowType(next_field)
        }
        return fields
    }

    // Just return the original input if we can't parse it
    return input
}

export interface ArrowTypeProps {
    type: string,
    name: string,
}

export default function ArrowTypeView ({ type, name }: ArrowTypeProps): ReactNode {
    const parsed_type = parseArrowType(type)
    if (parsed_type instanceof Object) {
        return (
            <ReactJson
                src={parsed_type}
                enableClipboard={false}
                theme={'grayscale'}
                displayDataTypes={false}
                displayObjectSize={false}
                quotesOnKeys={false}
                name={name + (type.startsWith('List(') ? '[List]' : '')}
                collapsed={true}
            />
        )
    } else {
        return (
            // Use the original type string if we don't parse an object out of it.
            <Text color='gray'>
                {type}
            </Text>
        )
    }
}