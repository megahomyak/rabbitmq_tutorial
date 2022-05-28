class RepeatingPrinter {
    private int lastPrintedMessageLength = 0;
    private int lineNumber = 1;

    final void print(String pattern, Object... formatters) {
        String stringToBePrinted = String.format(pattern, formatters);
        if (this.lineNumber != 1) {
            System.out.print('\r');
        }
        System.out.print(stringToBePrinted);
        for (
                int remainingSpacesAmount = this.lastPrintedMessageLength - stringToBePrinted.length();
                remainingSpacesAmount > 0;
                --remainingSpacesAmount
        ) {
            System.out.print(' ');
        }
        this.lastPrintedMessageLength = stringToBePrinted.length();
        ++this.lineNumber;
    }
}
