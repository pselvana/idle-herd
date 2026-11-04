import AppKit
import IdleHerdCore

/// Vector template artwork stays sharp at menu bar scale and follows macOS tint.
enum HerdMenuBarIcon {
    private static let images: [NSImage] = (0..<4).map { state in
        let image = NSImage(size: NSSize(width: 44, height: 18), flipped: false) { _ in
            NSColor.black.set()
            drawLlama(at: 0, filled: state & 1 != 0)
            drawLlama(at: 29, filled: state & 2 != 0)

            let arrow = NSBezierPath()
            arrow.move(to: NSPoint(x: 18, y: 7.5))
            arrow.line(to: NSPoint(x: 26, y: 7.5))
            arrow.move(to: NSPoint(x: 20.2, y: 9.7))
            arrow.line(to: NSPoint(x: 18, y: 7.5))
            arrow.line(to: NSPoint(x: 20.2, y: 5.3))
            arrow.move(to: NSPoint(x: 23.8, y: 9.7))
            arrow.line(to: NSPoint(x: 26, y: 7.5))
            arrow.line(to: NSPoint(x: 23.8, y: 5.3))
            arrow.lineWidth = 1.5
            arrow.lineCapStyle = .round
            arrow.lineJoinStyle = .round
            arrow.stroke()
            return true
        }
        image.isTemplate = true
        image.accessibilityDescription = "Two llamas connected by a bidirectional arrow"
        return image
    }

    static func image(for activity: MenuBarActivity) -> NSImage {
        images[(activity.localExecuting ? 1 : 0) | (activity.peerExecuting ? 2 : 0)]
    }

    private static func drawLlama(at offset: CGFloat, filled: Bool) {
        func point(_ x: CGFloat, _ y: CGFloat) -> NSPoint { NSPoint(x: x + offset, y: y) }
        let outline = NSBezierPath()
        outline.move(to: point(1, 1))
        outline.curve(to: point(2.5, 6.4), controlPoint1: point(1, 3.5), controlPoint2: point(1.6, 5))
        outline.line(to: point(3, 7.2))
        outline.curve(to: point(3, 11.2), controlPoint1: point(0.7, 8.6), controlPoint2: point(1.2, 10.1))
        outline.curve(to: point(3.2, 16.4), controlPoint1: point(2.3, 13.2), controlPoint2: point(2.2, 15.5))
        outline.curve(to: point(6.2, 12.8), controlPoint1: point(4.5, 18.3), controlPoint2: point(6.2, 16.2))
        outline.curve(to: point(8.8, 12.8), controlPoint1: point(7, 13.2), controlPoint2: point(8, 13.2))
        outline.curve(to: point(11.8, 16.4), controlPoint1: point(8.8, 16.2), controlPoint2: point(10.5, 18.3))
        outline.curve(to: point(12, 11.2), controlPoint1: point(12.8, 15.5), controlPoint2: point(12.7, 13.2))
        outline.curve(to: point(12, 7.2), controlPoint1: point(13.8, 10.1), controlPoint2: point(14.3, 8.6))
        outline.line(to: point(12.5, 6.4))
        outline.curve(to: point(14, 1), controlPoint1: point(13.4, 5), controlPoint2: point(14, 3.5))
        if filled {
            let silhouette = outline.copy() as! NSBezierPath
            silhouette.close()
            silhouette.fill()
        }
        outline.lineWidth = 1.5
        outline.lineCapStyle = .round
        outline.lineJoinStyle = .round
        outline.stroke()
    }
}
