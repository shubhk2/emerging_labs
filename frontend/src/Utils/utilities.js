export const getRandomBrightColor = () => {
    const hue = Math.floor(Math.random() * 360);
    const saturation = 100;
    const lightness = 50 + Math.floor(Math.random() * 20);
    return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
}
export const formatLabel = (label) => {
    return label
        .replace(/_/g, ' ')
        .replace(/\b\w/g, c => c.toUpperCase())
}
export const formatDateToShortMonthYear = (dateStr) => {
    const date = new Date(dateStr);
    const options = { month: 'short' };
    const month = new Intl.DateTimeFormat('en-US', options).format(date);
    const year = String(date.getFullYear()).slice(-2);
    return `${month} '${year}`;
}
export const splitOverview = (text = '', fullOverview = false) => {
    const match = text.match(/^For company (.+?) Company Overview Hide\s*/i);
    const companyName = match?.[1]?.trim() || '';
    const cleaned = text.replace(/^For company .+? Company Overview Hide\s*/i, '');
    const [intro = '', ...rest] = cleaned.split(/\n{2,}/);
    return {
        companyName,
        overview: intro.trim() + (fullOverview ? '\n\n' + rest.join('\n\n') : '')
    };
};
export const shortifyDecimalValue = (value, decimalPlaces = 1) => {
    if (typeof value === "number") {
        return Number.isInteger(value)
            ? value
            : Number(value.toFixed(decimalPlaces));
    }
    if (typeof value === "string") {
        if (value === '-' || value.trim() === '') return value;
        const match = value.match(/^([-\d.]+)([%A-Za-z]*)$/);
        if (!match) return value;
        const [, numStr, suffix] = match;
        const num = parseFloat(numStr);
        if (isNaN(num)) return value;
        const isEffectivelyInt = Number(num).toFixed(0) === numStr.replace(/^0+/, '').replace(/\.0+$/, '');
        const rounded = isEffectivelyInt
            ? Math.round(num).toString()
            : num.toFixed(decimalPlaces);
        return `${rounded}${suffix}`;
    }
    return value;
};
